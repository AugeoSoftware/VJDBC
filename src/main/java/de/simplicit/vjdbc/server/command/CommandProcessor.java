// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.server.command;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.simplicit.vjdbc.Registerable;
import de.simplicit.vjdbc.VJdbcException;
import de.simplicit.vjdbc.VJdbcProperties;
import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.command.ConnectionContext;
import de.simplicit.vjdbc.command.DestroyCommand;
import de.simplicit.vjdbc.command.StatementCancelCommand;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.server.config.ConnectionConfiguration;
import de.simplicit.vjdbc.server.config.OcctConfiguration;
import de.simplicit.vjdbc.server.config.VJdbcConfiguration;
import de.simplicit.vjdbc.util.PerformanceConfig;
import de.simplicit.vjdbc.util.SQLExceptionHelper;

/**
 * The CommandProcessor is a singleton class which dispatches calls from the
 * client to the responsible connection object.
 */
public class CommandProcessor {
    private static Log _logger = LogFactory.getLog(CommandProcessor.class);
    private static CommandProcessor _singleton;

    private static boolean closeConnectionsOnKill = true;
    private Timer _timer = null;
    private Map<Long, ConnectionEntry> _connectionEntries = new ConcurrentHashMap<Long, ConnectionEntry>();
    private OcctConfiguration _occtConfig;

    public static CommandProcessor getInstance() {
        if(_singleton == null) {
            _singleton = new CommandProcessor();
            installShutdownHook();
        }
        return _singleton;
    }

    public static void setDontCloseConnectionsOnKill()
    {
        closeConnectionsOnKill = false;
    }

    private CommandProcessor() {
        _occtConfig = VJdbcConfiguration.singleton().getOcctConfiguration();

        if(_occtConfig.getCheckingPeriodInMillis() > 0) {
            _logger.debug("OCCT starts");

            _timer = new Timer(true);
            _timer.scheduleAtFixedRate(new OrphanedConnectionCollectorTask(), _occtConfig.getCheckingPeriodInMillis(), _occtConfig
                    .getCheckingPeriodInMillis());
        } else {
            _logger.info("OCCT is turned off");
        }
    }

    private static void installShutdownHook() {
        // Install the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                getInstance().destroy();
            }
        }));
    }

    public UIDEx createConnection(String url, Properties props, Properties clientInfo, CallingContext ctx) throws SQLException {
        ConnectionConfiguration connectionConfiguration = VJdbcConfiguration.singleton().getConnection(url);

        if(connectionConfiguration != null) {
            _logger.debug("Found connection configuration " + connectionConfiguration.getId());

            Connection conn;
            try {
                conn = connectionConfiguration.create(props);
            } catch (VJdbcException e) {
                throw SQLExceptionHelper.wrap(e);
            }

            _logger.debug("Created connection, registering it now ...");

            UIDEx reg = registerConnection(conn, connectionConfiguration, clientInfo, ctx);

            if(_logger.isDebugEnabled()) {
                _logger.debug("Registered " + conn.getClass().getName() + " with UID " + reg);
            }

            return reg;
        } else {
            throw new SQLException("Can't find connection configuration for " + url);
        }
    }

    public ConnectionContext getConnectionEntry(Long connid) {
        if (connid!=null){
        	return _connectionEntries.get(connid);
        }
        return null;
    }

    public UIDEx registerConnection(Connection conn, ConnectionConfiguration config, Properties clientInfo, CallingContext ctx) {
        // To optimize the communication we can tell the client if
        // calling-contexts should be delivered at all
        int performanceProfile = PerformanceConfig.getPerformanceProfile(config.getCompressionModeAsInt(), config.getCompressionThreshold(), config.getRowPacketSize());
        UIDEx reg = new UIDEx(config.isTraceOrphanedObjects() ? 1 : 0, performanceProfile);
        _connectionEntries.put(reg.getUID(), new ConnectionEntry(reg.getUID(), conn, config, clientInfo, ctx));
        return reg;
    }

    public void registerJDBCObject(Long connid, Registerable obj)
    {
    	ConnectionContext entry = getConnectionEntry(connid);
        assert(entry != null);
        entry.addJDBCObject(obj.getReg().getUID(), obj);
    }

    public void unregisterJDBCObject(Long connid, Registerable obj)
    {
    	ConnectionContext entry = getConnectionEntry(connid);
        if (entry != null) {
            entry.removeJDBCObject(obj.getReg().getUID());
        }
    }

    public void destroy() {
        _logger.info("Destroying CommandProcessor ...");

        // Stop the timer
        if(_timer != null) {
            _timer.cancel();
        }

        if (closeConnectionsOnKill) {

            // Copy ConnectionEntries for closing
            ArrayList<ConnectionEntry> copyOfConnectionEntries = new ArrayList<ConnectionEntry>(_connectionEntries.values());
            // and clear the map immediately
            _connectionEntries.clear();

            for (ConnectionEntry connectionEntry: copyOfConnectionEntries){
				synchronized (connectionEntry) {
					connectionEntry.close();
				}
            }

        } else {
            _connectionEntries.clear();
        }
        _singleton = null;

        _logger.info("CommandProcessor successfully destroyed");
    }

    public Object process(Long connuid, Long uid, Command cmd, CallingContext ctx) throws SQLException {
        Object result = null;

        if(_logger.isDebugEnabled()) {
            _logger.debug(cmd);
        }

        if(connuid != null) {
            // Retrieving connection entry for the UID
            ConnectionEntry connentry = _connectionEntries.get(connuid);

            if(connentry != null) {
                try {
                    // StatementCancelCommand can be executed asynchronously to terminate
                    // a running query
                	if(cmd instanceof StatementCancelCommand) {
                        connentry.cancelCurrentStatementExecution(
                            connuid, uid, (StatementCancelCommand)cmd);
                    }
                    else {
                        // All other commands must be executed synchronously which is done
                        // by calling the synchronous executeCommand-Method
                        result = connentry.executeCommand(uid, cmd, ctx);
                    }
                } catch (SQLException e) {
                    if(_logger.isDebugEnabled()) {
                        _logger.debug("SQLException", e);
                    }
                    // Wrap the SQLException into something that can be safely thrown
                    throw SQLExceptionHelper.wrap(e);
                } catch (Throwable e) {
                    // Serious runtime error occured, wrap it in an SQLException
                    _logger.error("Unexpected",e);
                    throw SQLExceptionHelper.wrap(e);
                } finally {
                    // When there are no more JDBC objects left in the connection entry (that
                    // means even the JDBC-Connection is gone) the connection entry will be
                    // immediately destroyed and removed.
                    if(!connentry.hasJdbcObjects()) {
                        // As remove can be called asynchronously here, we must check the
                        // return value.
                        if(_connectionEntries.remove(connuid) != null) {
                            _logger.info("Connection " + connuid + " closed, statistics:");
                            connentry.traceConnectionStatistics();
                        }
                    }
                }
            } else {
                if(cmd instanceof DestroyCommand) {
                    _logger.debug("Connection entry already gone, DestroyCommand will be ignored");
                } else {
                    String msg = "Unknown connection entry " + connuid + " for command " + cmd.toString();
                    _logger.error(msg);
                    throw new SQLException(msg);
                }
            }
        } else {
            String msg = "Connection id is null";
            _logger.fatal(msg);
            throw new SQLException(msg);
        }

        return result;
    }

    /**
     * release all connections established by given user, except the given one.
     * @param connectionEntry
     * @param userName
     * @return number of released connections
     */
    public int releaseUserConnections(ConnectionContext connectionEntry, String userName)  {
    	int killedConnectionsCount = 0;
    	if (userName!=null){
    		_logger.debug("Killing connections for user "+userName);
//    		synchronized(_connectionEntries){
    			Iterator<Entry<Long, ConnectionEntry>> it = _connectionEntries.entrySet().iterator();
    			while (it.hasNext()){
    				Entry<Long, ConnectionEntry> me = it.next();
    				ConnectionEntry ce = me.getValue();
    				synchronized(ce){
    					if (ce!=connectionEntry 
    							&& userName.equals(ce.getClientInfo().getProperty(VJdbcProperties.USER_NAME))){
    						
    						try {
    							DestroyCommand.INSTANCE.execute(ce.getJDBCObject(me.getKey()), ce);
    							it.remove();
    							killedConnectionsCount++;						
							} catch (SQLException e) {
								_logger.info("Error on killing connection "+me.getKey(), e);
							}
    					}
    				}    				
    			}    			
//    		}
    	}
    	return killedConnectionsCount;
    }
    
    /**
     * The orphaned connection collector task periodically checks the existing
     * connection entries for orphaned entries that means connections which
     * weren't used for a specific time and where the client didn't send keep
     * alive pings.
     */
    private class OrphanedConnectionCollectorTask extends TimerTask {
        public void run() {
            try {
                _logger.debug("Checking for orphaned connections ...");

                long millis = System.currentTimeMillis();

                for(Iterator<Long> it = _connectionEntries.keySet().iterator(); it.hasNext();) {
                    Long key = it.next();
                    ConnectionEntry connentry = _connectionEntries.get(key);

                    // Synchronize here so that the process-Method doesn't
                    // access the same entry concurrently
                    synchronized (connentry) {
                        long idleTime = millis - connentry.getLastAccess();

                        if(!connentry.isActive() && (idleTime > _occtConfig.getTimeoutInMillis())) {
                            _logger.info("Closing orphaned connection " + key + " after being idle for about " + (idleTime / 1000) + "sec");
                            // The close method doesn't throw an exception
                            connentry.close();
                            it.remove();
                        }
                    }
                }
            } catch (ConcurrentModificationException e) {
                // This exception might happen when the process-Method has changed the
                // connection-entry map while this iteration is ongoing. We just ignore the
                // exception and let the entry stay in the map until the next run of the
                // OCCT (the last access time isn't modified until the next run).
                if(_logger.isDebugEnabled()) {
                    String msg = "ConcurrentModificationException in OCCT";
                    _logger.debug(msg, e);
                }
            } catch (RuntimeException e) {
                // Any other error will be propagated so that the timer task is stopped
                String msg = "Unexpected Runtime-Exception in OCCT";
                _logger.fatal(msg, e);
            }
        }
    }
}
