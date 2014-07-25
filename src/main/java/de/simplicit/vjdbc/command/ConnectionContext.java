// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.sql.SQLException;
import java.util.Properties;

/**
 * This interface provides access to connection specific context for all commands
 * executed on the server.
 */
public interface ConnectionContext {
    // Accessor methods to all registered JDBC objects
    Object getJDBCObject(Long key);
    void addJDBCObject(Long key, Object partner);
    Object removeJDBCObject(Long key);
    // Compression
    int getCompressionMode();
    int getCompressionThreshold();
    // Row-Packets
    int getRowPacketSize();
    String getCharset();
    // Resolve and check query
    String resolveOrCheckQuery(String sql) throws SQLException;
    // convenience method to remove all related JdbcObjects from this connection
    void closeAllRelatedJdbcObjects() throws SQLException;
    // client info properties
    Properties getClientInfo();
    // set client info 
    void setClientInfo(String name, String value);
}
