//VJDBC - Virtual JDBC
//Written by Michael Link
//Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.server.command;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.simplicit.vjdbc.serial.RowPacket;
import de.simplicit.vjdbc.server.config.ConnectionConfiguration;

/**
 * The ResultSetHolder is responsible to hold a reference to an open ResultSet.
 * It reads succeeding RowPackets in a Worker-Thread to immediately return a
 * result when nextRowPacket is called.
 */
public class ResultSetHolder {
    private static Log _logger = LogFactory.getLog(ResultSetHolder.class);

    private final Object _lock = new Object();
    private boolean _readerThreadIsRunning = false;
    private volatile int packetIndex = 1; // first packe is already sent with StreamingResultSet 

    private final ResultSet _resultSet;
    private final ResultSetMetaData _metaData;
    private RowPacket _currentSerializedRowPacket;
    private final ConnectionConfiguration _connectionConfiguration;
    private volatile boolean _lastPartReached;
    private final int _rowPacketSize;
    private SQLException _lastOccurredException = null;

    ResultSetHolder(ResultSet resultSet, ResultSetMetaData metaData, ConnectionConfiguration config, int rowPacketSize, boolean lastPartReached) throws SQLException {
        _resultSet = resultSet;
        _metaData = metaData;
        _connectionConfiguration = config;
        _rowPacketSize = rowPacketSize;
        _lastPartReached = lastPartReached;
        if(!_lastPartReached) {
            synchronized(_lock) {
                readNextRowPacket();
            }
        }
    }

    public ResultSetMetaData getMetaData() throws SQLException {
    	return _metaData;
    }

    public void close() throws SQLException {
        synchronized (_lock) {
            _resultSet.close();
//            _resultSet = null;
        }
    }

    public RowPacket nextRowPacket() throws SQLException {
        synchronized (_lock) {
            // If the reader thread is still running we must wait
            // for the lock to be released by the reader
            while(_readerThreadIsRunning) {
                try {
                    // Wait for the reader thread to finish
                    _lock.wait();
                } catch (InterruptedException e) {
                    String msg = "Reader thread interrupted unexpectedly";
                    // Some unexpected exception occured, we must leave the loop here as the
                    // termination flag might not be reset to false.
                    _logger.error(msg, e);
                    _lastOccurredException = new SQLException(msg);
                    break;
                }
            }

            // If any exception occured in the worker thread it will
            // be delivered to the client as a normal SQL exception
            if(_lastOccurredException != null) {
                throw _lastOccurredException;
            }

            // Remember current row packet as the result
            RowPacket result = _currentSerializedRowPacket;
            // Start next reader thread
            readNextRowPacket();
            // Return the result
            return result;
        }
    }

    private void readNextRowPacket() throws SQLException {
        if(_resultSet != null && !_lastPartReached) {
            // Start the thread
            try {
                _connectionConfiguration.execute(new Runnable() {
                    public void run() {
                        // Aquire lock immediately
                        synchronized (_lock) {
                            try {
                                // When the ResultSet is null here, the client closed the ResultSet concurrently right
                                // after the upper check "_resultSet != null".
                                if(_resultSet != null) {
                                    RowPacket rowPacket = new RowPacket(_rowPacketSize/*, false*/, packetIndex);
                                    packetIndex++;
                                    // Populate the new RowPacket using the ResultSet
                                    _lastPartReached = rowPacket.populate(_resultSet, _metaData);
                                    _currentSerializedRowPacket = rowPacket;
                                }
                            } catch (SQLException e) {
                                // Just remember the exception, it will be thrown at
                                // the next call to nextRowPacket
                                _lastOccurredException = e;
                            } finally {
                                _readerThreadIsRunning = false;
                                // Notify possibly waiting subsequent Readers
                                _lock.notify();
                            }
                        }
                    }
                });

                // Set the flag that the reader thread is considered to be running.
                _readerThreadIsRunning = true;
            } catch (RejectedExecutionException e) {
                String msg = "Read next row packet task was rejected unexpectedly";
                _logger.error(msg, e);
                throw new SQLException(msg);
            }
        } else {
            _currentSerializedRowPacket = null;
        }
    }
}
