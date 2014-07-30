// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.server.command.ResultSetHolder;

public class DestroyCommand implements Command {
    static final long serialVersionUID = 4457392123395584636L;

    private static Log _logger = LogFactory.getLog(DestroyCommand.class);
    private Long _uid;
    private int _interfaceType;

    public DestroyCommand() {
    }

    public DestroyCommand(UIDEx regentry, int interfaceType) {
        _uid = regentry.getUID();
        _interfaceType = interfaceType;
    }

    public DestroyCommand(Long uid, int interfaceType) {
    	this._uid = uid;
    	this._interfaceType = interfaceType;
    }
    
    public Long getUID() {
		return _uid;
	}

	public int getInterfaceType() {
		return _interfaceType;
	}

	public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_uid.longValue());
        out.writeInt(_interfaceType);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _uid = new Long(in.readLong());
        _interfaceType = in.readInt();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
    	/*
    	 * if we are trying to close a Connection, we also need to close all the other associated
    	 * JDBC objects, such as ResultSet, Statements, etc.
    	 */
    	if(target instanceof Connection) {
    		if(_logger.isDebugEnabled()) {
    			_logger.debug("******************************************************");
    			_logger.debug("Destroy command for Connection found!");
    			_logger.debug("destroying and closing all related JDBC objects first.");
    			_logger.debug("******************************************************");
    		}
    		ctx.closeAllRelatedJdbcObjects();
    	}
    	// now we are ready to go on and close this connection
    	
        Object removed = ctx.removeJDBCObject(_uid);

        // Check for identity
        if(removed == target) {
            if(_logger.isDebugEnabled()) {
                _logger.debug("Removed " + target.getClass().getName() + " with UID " + _uid);
            }
            try {
	            switch(_interfaceType){
	            case JdbcInterfaceType.RESULTSETHOLDER:
	            	((ResultSetHolder)target).close();
	            	break;
	            case JdbcInterfaceType.STATEMENT:
	            	((Statement)target).close();
	            	break;
	            case JdbcInterfaceType.CONNECTION:
	            	((Connection)target).close();
	            	break;
	            case JdbcInterfaceType.CALLABLESTATEMENT:
	            	((CallableStatement)target).close();
	            	break;
	            case JdbcInterfaceType.DATABASEMETADATA:
	            	_logger.debug("close() not supported for DatabaseMetaData");
	            	break;
	            case JdbcInterfaceType.PREPAREDSTATEMENT:
	            	((PreparedStatement)target).close();
	            	break;
	            case JdbcInterfaceType.SAVEPOINT:
	            	_logger.debug("close() not supported for Savepoint");
	            	break;
	            default:
	            	_logger.debug("Unknown interfaceType: "+_interfaceType);            
	            }
            } catch (SQLException e){
            	throw e;
            } catch (Exception e){
            	_logger.debug("Invocation of close() failed", e);
            }
        } else {
            if(_logger.isWarnEnabled()) {
                _logger.warn("Target object " + target + " wasn't registered with UID " + _uid);
            }
        }
        return null;
    }

    public String toString() {
        return "DestroyCommand";
    }

}
