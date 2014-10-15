// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.sql.SQLException;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.simplicit.vjdbc.rmi.KeepAliveTimerTask;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.server.command.CompositeCommand;

/**
 * The DecoratedCommandSink makes it easier to handle the CommandSink. It contains a number
 * of different utility methods which wrap parameters, unwrap results and so on. Additionally
 * it supports a Listener which is called before and after execution of the command.
 */
public class DecoratedCommandSink {
    private static Log _logger = LogFactory.getLog(DecoratedCommandSink.class);

    private final UIDEx _connectionUid;
    private final CommandSink _targetSink;
    private CommandSinkListener _listener = new NullCommandSinkListener();
    private CallingContextFactory _callingContextFactory;
    private Timer _timer;
    private final ExecutorService _executor;
    private ThreadLocal<CompositeCommand> _compositeCommand = new ThreadLocal<CompositeCommand>();

    public DecoratedCommandSink(UIDEx connuid, CommandSink sink, CallingContextFactory ctxFactory) {
        this(connuid, sink, ctxFactory, 10000l);
    }

    public DecoratedCommandSink(UIDEx connuid, CommandSink sink, CallingContextFactory ctxFactory, long pingPeriod) {
        _connectionUid = connuid;
        _targetSink = sink;
        _callingContextFactory = ctxFactory;

        if (pingPeriod > 0) {
            _timer = new Timer(true);

            // Schedule the keep alive timer task
            KeepAliveTimerTask task = new KeepAliveTimerTask(this);
            _timer.scheduleAtFixedRate(task, pingPeriod, pingPeriod);
        }
        _executor = Executors.newSingleThreadExecutor();
    }

    public CommandSink getTargetSink()
    {
        return _targetSink;
    }

    public void close() {
        // Stop the keep-alive timer
        if (_timer != null) {
            _timer.cancel();
            _timer = null;
        }
        // Stop executor
        _executor.shutdown();
        try {
			_executor.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			_logger.debug("Executor service shotdown has beed interrupted", e);
		}
        // Close down the sink
        _targetSink.close();
    }

    public void setListener(CommandSinkListener listener) {
        if(listener != null) {
            _listener = listener;
        } else {
            _listener = new NullCommandSinkListener();
        }
    }

    public UIDEx connect(String url, Properties props, Properties clientInfo, CallingContext ctx) throws SQLException {
        return _targetSink.connect(url, props, clientInfo, ctx);
    }

    public Object process(UIDEx reg, Command cmd) throws SQLException {
        return process(reg, cmd, false);
    }

    public Object process(UIDEx reg, Command cmd, boolean withCallingContext) throws SQLException {
    	try {
            CallingContext ctx = null;
            if(withCallingContext) {
                ctx = _callingContextFactory.create();
            }
            CompositeCommand compositeCommand = _compositeCommand.get();
            if (compositeCommand!=null){
            	compositeCommand.add(reg, cmd);
            	cmd = compositeCommand;
            	reg = null;
            }
            _listener.preExecution(cmd);
            Object result = _targetSink.process(_connectionUid.getUID(), reg != null ? reg.getUID() : null, cmd, ctx);
            if (compositeCommand!=null){
            	compositeCommand.updateResultUIDEx((Object[]) result);
            	result = ((Object[]) result)[compositeCommand.size()-1];
            	_compositeCommand.set(null);
            }
			return result;
        } finally {
            _listener.postExecution(cmd);
        }
    }

    public UIDEx queue(final UIDEx reg, final Command cmd, final boolean withCallingContext){
    	CompositeCommand compositeCommand = _compositeCommand.get();
    	if (compositeCommand==null){
    		compositeCommand = new CompositeCommand();
    		_compositeCommand.set(compositeCommand);
    	}
    	return compositeCommand.add(reg, cmd);
    }
    
    public Future<Object> processAsync(final UIDEx reg, final Command cmd, final boolean withCallingContext){
    	final CallingContext ctx = withCallingContext?_callingContextFactory.create():null;
    	return _executor.submit(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
		    	try {
		            _listener.preExecution(cmd);
		            return _targetSink.process(_connectionUid.getUID(), reg != null ? reg.getUID() : null, cmd, ctx);
				} finally {
		            _listener.postExecution(cmd);
		        }
			}
		});
    }
    
    public int processWithIntResult(UIDEx uid, Command cmd) throws SQLException {
    	return ((Integer)process(uid, cmd, false)).intValue();
    }

    public int processWithIntResult(UIDEx uid, Command cmd, boolean withCallingContext) throws SQLException {
    	return ((Integer)process(uid, cmd, withCallingContext)).intValue();
    }

    public boolean processWithBooleanResult(UIDEx uid, Command cmd) throws SQLException {
    	return ((Boolean)process(uid,cmd,false)).booleanValue();
    }

    public boolean processWithBooleanResult(UIDEx uid, Command cmd, boolean withCallingContext) throws SQLException {
    	return ((Boolean)process(uid,cmd,withCallingContext)).booleanValue();
    }

    public byte processWithByteResult(UIDEx uid, Command cmd) throws SQLException {
    	return ((Byte)process(uid,cmd,false)).byteValue();
    }

    public byte processWithByteResult(UIDEx uid, Command cmd, boolean withCallingContext) throws SQLException {
    	return ((Byte)process(uid,cmd,withCallingContext)).byteValue();
    }

    public short processWithShortResult(UIDEx uid, Command cmd) throws SQLException {
    	return ((Short)process(uid,cmd,false)).shortValue();

    }

    public short processWithShortResult(UIDEx uid, Command cmd, boolean withCallingContext) throws SQLException {
    	return ((Short)process(uid,cmd,withCallingContext)).shortValue();
    }

    public long processWithLongResult(UIDEx uid, Command cmd) throws SQLException {
    	return ((Long)process(uid,cmd,false)).longValue();

    }

    public long processWithLongResult(UIDEx uid, Command cmd, boolean withCallingContext) throws SQLException {
    	return ((Long)process(uid,cmd,withCallingContext)).longValue();
    }

    public float processWithFloatResult(UIDEx uid, Command cmd) throws SQLException {
    	return ((Float)process(uid,cmd,false)).floatValue();

    }

    public float processWithFloatResult(UIDEx uid, Command cmd, boolean withCallingContext) throws SQLException {
    	return ((Float)process(uid,cmd,withCallingContext)).floatValue();
    }

    public double processWithDoubleResult(UIDEx uid, Command cmd) throws SQLException {
    	return ((Double)process(uid,cmd,false)).doubleValue();
    }

    public double processWithDoubleResult(UIDEx uid, Command cmd, boolean withCallingContext) throws SQLException {
    	return ((Double)process(uid,cmd,withCallingContext)).doubleValue();
    }
}
