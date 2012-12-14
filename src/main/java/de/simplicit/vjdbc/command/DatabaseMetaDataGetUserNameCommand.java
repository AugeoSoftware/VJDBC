package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import de.simplicit.vjdbc.VJdbcProperties;

public class DatabaseMetaDataGetUserNameCommand implements Command {

	static final long serialVersionUID = 3543492350930057039L;;
	
	
	public DatabaseMetaDataGetUserNameCommand() {
		
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	}

	public Object execute(Object target, ConnectionContext ctx) throws SQLException {
		Object userName = ctx.getClientInfo().get(VJdbcProperties.USER_NAME);
		if (userName==null || "".equals(userName)){
			userName = ((DatabaseMetaData)target).getUserName();
		}
		return userName;
	}

}
