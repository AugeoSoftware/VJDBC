// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.server.command;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.simplicit.vjdbc.ProxiedObject;
import de.simplicit.vjdbc.VJdbcProperties;
import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.command.ConnectionContext;
import de.simplicit.vjdbc.command.DestroyCommand;
import de.simplicit.vjdbc.command.JdbcInterfaceType;
import de.simplicit.vjdbc.command.ResultSetProducerCommand;
import de.simplicit.vjdbc.command.StatementCancelCommand;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.SerialDatabaseMetaData;
import de.simplicit.vjdbc.serial.SerialResultSetMetaData;
import de.simplicit.vjdbc.serial.StreamingResultSet;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.server.config.ConfigurationException;
import de.simplicit.vjdbc.server.config.ConnectionConfiguration;
import de.simplicit.vjdbc.server.config.VJdbcConfiguration;
import de.simplicit.vjdbc.util.ClientInfo;
import de.simplicit.vjdbc.util.PerformanceConfig;

class ConnectionEntry implements ConnectionContext {
    private static Log _logger = LogFactory.getLog(ConnectionEntry.class);

    // Unique identifier for the ConnectionEntry
    private final Long _uid;
    // The real JDBC-Connection
    private final Connection _connection;
    // Configuration information
    private final ConnectionConfiguration _connectionConfiguration;
    // Properties delivered from the client
    private Properties _clientInfo;
    // Flag that signals the activity of this connection
    private volatile boolean _active = false;
    // data compression mode [0..9] for this connection
    private volatile int _compressionMode;
    // data compression threshold [0..4000] for this connection
    private volatile int _compressionThreshold;
    // row packet size for this connection
    private volatile int _rowPacketSize;

    // Statistics
    private volatile long _lastAccessTimestamp = System.currentTimeMillis();
    private long _numberOfProcessedCommands = 0;

    // Map containing all JDBC-Objects which are created by this Connection
    // entry
    private Map<Long, JdbcObjectHolder> _jdbcObjects = new ConcurrentHashMap<Long, JdbcObjectHolder>();
    // Map for counting commands
    private ConcurrentMap<String, AtomicInteger> _commandCountMap = new ConcurrentHashMap<String, AtomicInteger>();

    ConnectionEntry(Long connuid, Connection conn, ConnectionConfiguration config, Properties clientInfo, CallingContext ctx) {
        _connection = conn;
        _connectionConfiguration = config;
        _clientInfo = clientInfo;
        _uid = connuid;
        _compressionMode = _connectionConfiguration.getCompressionModeAsInt();
        _compressionThreshold = _connectionConfiguration.getCompressionThreshold();
        _rowPacketSize = _connectionConfiguration.getRowPacketSize();
        
        // Put the connection into the JDBC-Object map
        _jdbcObjects.put(connuid, new JdbcObjectHolder(conn, ctx, JdbcInterfaceType.CONNECTION));
    }

    void close() {
        try {
            if(!_connection.isClosed()) {
                _connection.close();

                if(_logger.isDebugEnabled()) {
                    _logger.debug("Closed connection " + _uid);
                }
            }

            traceConnectionStatistics();
        } catch (SQLException e) {
            _logger.error("Exception during closing connection", e);
        }
    }

    public void closeAllRelatedJdbcObjects() throws SQLException {
    	for (Map.Entry<Long, JdbcObjectHolder> me: _jdbcObjects.entrySet()){
    		JdbcObjectHolder jdbcObject = me.getValue();
    		// don't act on the Connection itself - this will be done elsewhere
    		if(jdbcObject.getJdbcInterfaceType() == JdbcInterfaceType.CONNECTION)
    			continue;
    		// create a DestroyCommand and act on it
    		DestroyCommand.INSTANCE.execute(jdbcObject.getJdbcObject(), this);
    	}
    	_jdbcObjects.clear();    	
    }
    
    boolean hasJdbcObjects() {
        return !_jdbcObjects.isEmpty();
    }

    public Properties getClientInfo() {
        return _clientInfo;
    }

    public void setClientInfo(String name, String value){
    	_clientInfo.setProperty(name, value);
    	if (VJdbcProperties.PERFORMANCE_PROFILE.equals(name)){
    		try {
				int performanceProfile = Integer.parseInt(value);
				_compressionMode = PerformanceConfig.getCompressionMode(performanceProfile);
				_compressionThreshold = PerformanceConfig.getCompressionThreshold(performanceProfile);
				_rowPacketSize = PerformanceConfig.getRowPacketSize(performanceProfile);
    		} catch (NumberFormatException e) {
				_logger.debug("Ignoring invalid number format for performance profile from client "+_clientInfo.getProperty(ClientInfo.VJDBC_CLIENT_ADDRESS), e);
    		} catch (ConfigurationException e) {
    			_logger.debug("Ignoring invalid performance profile from client "+_clientInfo.getProperty(ClientInfo.VJDBC_CLIENT_ADDRESS), e);
    		}
    	} else 
    		
    	if (VJdbcProperties.COMPRESSION_MODE.equals(name)){
    		try {
				_compressionMode = PerformanceConfig.parseCompressionMode(value);
			} catch (ConfigurationException e) {
				_logger.debug("Ignoring invalid compression mode from client "+_clientInfo.getProperty(ClientInfo.VJDBC_CLIENT_ADDRESS), e);
			}
    	} else 
    		
   		if (VJdbcProperties.COMPRESSION_THRESHOLD.equals(name)){
    		try {
				_compressionThreshold = PerformanceConfig.parseCompressionThreshold(value);
			} catch (ConfigurationException e) {
				_logger.debug("Ignoring invalid compression threshold from client "+_clientInfo.getProperty(ClientInfo.VJDBC_CLIENT_ADDRESS), e);
			}   			
   		} else 
   			
    	if (VJdbcProperties.ROW_PACKET_SIZE.equals(name)){
    		try {
				_rowPacketSize = PerformanceConfig.parseRowPacketSize(value);
			} catch (ConfigurationException e) {
				_logger.debug("Ignoring invalid row packet size from client "+_clientInfo.getProperty(ClientInfo.VJDBC_CLIENT_ADDRESS), e);
			}
    	}
    }
    
    public boolean isActive() {
        return _active;
    }

    public long getLastAccess() {
        return _lastAccessTimestamp;
    }

    public long getNumberOfProcessedCommands() {
        return _numberOfProcessedCommands;
    }

    public Object getJDBCObject(Long key) {
        JdbcObjectHolder jdbcObjectHolder = _jdbcObjects.get(key);
        if(jdbcObjectHolder != null) {
            return jdbcObjectHolder.getJdbcObject();
        } else {
            return null;
        }
    }

    public void addJDBCObject(Long key, Object partner) {
    	int _jdbcInterfaceType = getJdbcInterfaceTypeFromObject(partner);
        _jdbcObjects.put(key, new JdbcObjectHolder(partner, null, _jdbcInterfaceType));
    }

    public Object removeJDBCObject(Long key) {
        JdbcObjectHolder jdbcObjectHolder = _jdbcObjects.remove(key);
        if(jdbcObjectHolder != null) {
            return jdbcObjectHolder.getJdbcObject();
        } else {
            return null;
        }
    }

    public int getCompressionMode() {
        return _compressionMode;
    }

    public int getCompressionThreshold() {
        return _compressionThreshold;
    }

    public int getRowPacketSize() {
        return _rowPacketSize;
    }

    public String getCharset() {
        return _connectionConfiguration.getCharset();
    }

    public String resolveOrCheckQuery(String sql) throws SQLException
    {
        if (sql.startsWith("$")) {
            return getNamedQuery(sql.substring(1));
        }
        else {
            checkAgainstQueryFilters(sql);
            return sql;
        }
    }

    public synchronized Object executeCommand(Long uid, Command cmd, CallingContext ctx) throws SQLException {
        try {
            _active = true;
            _lastAccessTimestamp = System.currentTimeMillis();

            Object result = null;

            // Some target object ?
            if(uid != null) {
                // ... get it
            	JdbcObjectHolder target = (cmd instanceof DestroyCommand)?_jdbcObjects.remove(uid):_jdbcObjects.get(uid);

                if(target != null) {
                    if(_logger.isDebugEnabled()) {
                        _logger.debug("Target for UID " + uid + " found");
                    }
                    // Execute the command on the target object
                    result = cmd.execute(target.getJdbcObject(), this);
                    // Check if the result must be remembered on the server side with a UID
                    UIDEx uidResult = ReturnedObjectGuard.checkResult(result);

                    if(uidResult != null) {
                        // put it in the JDBC-Object-Table
                    	int _jdbcInterfaceType = getJdbcInterfaceTypeFromObject(result);
                        _jdbcObjects.put(uidResult.getUID(), new JdbcObjectHolder(result, ctx, _jdbcInterfaceType));
                        if(_logger.isDebugEnabled()) {
                            _logger.debug("Registered " + result.getClass().getName() + " with UID " + uidResult);
                        }
                        
                        if (result instanceof DatabaseMetaData){
                        	return new SerialDatabaseMetaData(uidResult, (DatabaseMetaData) result);
                        }                        
                        if (result instanceof ProxiedObject) {
                            return ((ProxiedObject)result).getProxy();
                        }
                        return uidResult;
                    } else {
                        // When the result is of type ResultSet then handle it specially
                        if(result != null &&
                           VJdbcConfiguration.getUseCustomResultSetHandling()) {
                            if(result instanceof ResultSet) {
                                boolean forwardOnly = false;
                                if(cmd instanceof ResultSetProducerCommand) {
                                    forwardOnly = ((ResultSetProducerCommand) cmd).getResultSetType() == ResultSet.TYPE_FORWARD_ONLY;
                                } else {
                                    _logger.debug("Command " + cmd.toString() + " doesn't implement "
                                            + "ResultSetProducer-Interface, assuming ResultSet is scroll insensitive");
                                }
                                result = handleResultSet((ResultSet) result, forwardOnly, ctx);
                            } else if(result instanceof ResultSetMetaData) {
                                result = handleResultSetMetaData((ResultSetMetaData) result);
                            } else {
                                if(_logger.isDebugEnabled()) {
                                    _logger.debug("... returned " + result);
                                }
                            }
                        }
                    }
                } else {
                    _logger.warn("JDBC-Object for UID " + uid + " and command " + cmd + " is null !");
                }
            } else {
                result = cmd.execute(null, this);
            }

            if(_connectionConfiguration.isTraceCommandCount()) {
                String cmdString = cmd.toString();
                AtomicInteger value = new AtomicInteger(1);
                value = _commandCountMap.putIfAbsent(cmdString, value);
                if (value!=null){
                	value.incrementAndGet();
                }
            }

            _numberOfProcessedCommands++;

            return result;
        } finally {
            _active = false;
            _lastAccessTimestamp = System.currentTimeMillis();
        }
    }

    public void cancelCurrentStatementExecution(
        Long connuid, Long uid, StatementCancelCommand cmd)
        throws SQLException {
        // Get the Statement object
        JdbcObjectHolder target = _jdbcObjects.get(uid);

        if (target != null) {
            try {
                Statement stmt = (Statement)target.getJdbcObject();
                if (stmt != null) {
                    cmd.execute(stmt, this);
                } else {
                    _logger.info("no statement with id " + uid + " to cancel");
                }
            } catch (Exception e) {
                _logger.info(e.getMessage(), e);
            }
        } else {
            _logger.info("no statement with id " + uid + " to cancel");
        }
    }
    
    public void traceConnectionStatistics() {
        _logger.info("  Connection ........... " + _connectionConfiguration.getId());
        _logger.info("  IP address ........... " + _clientInfo.getProperty(ClientInfo.VJDBC_CLIENT_ADDRESS, "n.a."));
        _logger.info("  Host name ............ " + _clientInfo.getProperty(ClientInfo.VJDBC_CLIENT_NAME, "n.a."));
        dumpClientInfoProperties();
        _logger.info("  Last time of access .. " + new Date(_lastAccessTimestamp));
        _logger.info("  Processed commands ... " + _numberOfProcessedCommands);

        if(_jdbcObjects.size() > 0) {
            _logger.info("  Remaining objects .... " + _jdbcObjects.size());
            for(Iterator<JdbcObjectHolder> it = _jdbcObjects.values().iterator(); it.hasNext();) {
                JdbcObjectHolder jdbcObjectHolder = it.next();
                _logger.info("  - " + jdbcObjectHolder.getJdbcObject().getClass().getName());
                if(_connectionConfiguration.isTraceOrphanedObjects()) {
                    if(jdbcObjectHolder.getCallingContext() != null) {
                        _logger.info(jdbcObjectHolder.getCallingContext().getStackTrace());
                    }
                }
            }
        }

        if(!_commandCountMap.isEmpty()) {
            _logger.info("  Command-Counts:");

            ArrayList<Map.Entry<String, AtomicInteger>> entries = new ArrayList<Map.Entry<String, AtomicInteger>>(_commandCountMap.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<String, AtomicInteger>>() {
                public int compare(Entry<String, AtomicInteger> o1, Entry<String, AtomicInteger> o2) {
                    // Descending sort
                    return -Integer.valueOf(o1.getValue().intValue()).compareTo(Integer.valueOf(o2.getValue().intValue()));
                }
            });

            for (Map.Entry<String, AtomicInteger> me: entries){
                _logger.info("     " + me.getValue().intValue() + " : " + me.getKey());
            }
        }
    }

    private Object handleResultSet(ResultSet result, boolean forwardOnly, CallingContext ctx) throws SQLException {
        // Populate a StreamingResultSet
        StreamingResultSet srs = new StreamingResultSet(
                _rowPacketSize,
                forwardOnly,
                _connectionConfiguration.isPrefetchResultSetMetaData(),
                _connectionConfiguration.getCharset());
        // Populate it
        ResultSetMetaData metaData = result.getMetaData();
        boolean lastPartReached = srs.populate(result, metaData);
        // Remember the ResultSet and put the UID in the StreamingResultSet
        UIDEx uid = new UIDEx();
        srs.setRemainingResultSetUID(uid);
        _jdbcObjects.put(uid.getUID(), new JdbcObjectHolder(new ResultSetHolder(result, metaData, _connectionConfiguration, _rowPacketSize, lastPartReached), ctx, JdbcInterfaceType.RESULTSETHOLDER));
        if(_logger.isDebugEnabled()) {
            _logger.debug("Registered ResultSet with UID " + uid.getUID());
        }
        return srs;
    }

    private Object handleResultSetMetaData(ResultSetMetaData result) throws SQLException {
        return new SerialResultSetMetaData(result);
    }

    private void dumpClientInfoProperties() {
        if(_logger.isInfoEnabled() && !_clientInfo.isEmpty()) {
            boolean printedHeader = false;

            for(Enumeration it = _clientInfo.keys(); it.hasMoreElements();) {
                String key = (String) it.nextElement();
                if(!key.startsWith("vjdbc")) {
                    if(!printedHeader) {
                        printedHeader = true;
                        _logger.info("  Client-Properties ...");
                    }
                    _logger.info("    " + key + " => " + _clientInfo.getProperty(key));
                }
            }
        }
    }

    private String getNamedQuery(String id) throws SQLException {
        if(_connectionConfiguration.getNamedQueries() != null) {
            return _connectionConfiguration.getNamedQueries().getSqlForId(this, id);
        } else {
            String msg = "No named-queries are associated with this connection";
            _logger.error(msg);
            throw new SQLException(msg);
        }
    }

    private void checkAgainstQueryFilters(String sql) throws SQLException {
        if(_connectionConfiguration.getQueryFilters() != null) {
            _connectionConfiguration.getQueryFilters().checkAgainstFilters(sql);
        }
    }
    
    private int getJdbcInterfaceTypeFromObject(Object jdbcObject) {
    	int _jdbcInterfaceType = 0;
    	if(jdbcObject == null) {
    		return _jdbcInterfaceType;
    	}
    	if(jdbcObject instanceof CallableStatement) {
    		_jdbcInterfaceType = JdbcInterfaceType.CALLABLESTATEMENT;
    	} else if(jdbcObject instanceof Connection) {
    		_jdbcInterfaceType = JdbcInterfaceType.CONNECTION;    		
    	} else if(jdbcObject instanceof DatabaseMetaData) {
    		_jdbcInterfaceType = JdbcInterfaceType.DATABASEMETADATA;
    	} else if(jdbcObject instanceof PreparedStatement) {
    		_jdbcInterfaceType = JdbcInterfaceType.PREPAREDSTATEMENT;
    	} else if(jdbcObject instanceof Savepoint) {
    		_jdbcInterfaceType = JdbcInterfaceType.SAVEPOINT;
    	} else if(jdbcObject instanceof Statement) {
    		_jdbcInterfaceType = JdbcInterfaceType.STATEMENT;
    	} else if(jdbcObject instanceof ResultSetHolder) {
    		_jdbcInterfaceType = JdbcInterfaceType.RESULTSETHOLDER;
    	}
    	return _jdbcInterfaceType;
    }
}
