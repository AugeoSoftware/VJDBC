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

    public static final DestroyCommand INSTANCE = new DestroyCommand();
    
    private static Log _logger = LogFactory.getLog(DestroyCommand.class);

    private DestroyCommand() {
    }


	public void writeExternal(ObjectOutput out) throws IOException {
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
    	
    	if (target instanceof ResultSetHolder){
    		((ResultSetHolder)target).close();
    	} else 
    	if (target instanceof Statement){
    		((Statement)target).close();
    	} else     	
    	if(target instanceof Connection) {
    		/*
    		 * if we are trying to close a Connection, we also need to close all the other associated
    		 * JDBC objects, such as ResultSet, Statements, etc.
    		 */
    		if(_logger.isDebugEnabled()) {
    			_logger.debug("******************************************************");
    			_logger.debug("Destroy command for Connection found!");
    			_logger.debug("destroying and closing all related JDBC objects first.");
    			_logger.debug("******************************************************");
    		}
    		ctx.closeAllRelatedJdbcObjects();
    		((Connection)target).close();
		} else {
			if (_logger.isDebugEnabled()) {
				_logger.debug("close() not supported for target object " + target);
			}
		}  
    	return null;
    }

    public String toString() {
        return "DestroyCommand";
    }

}
