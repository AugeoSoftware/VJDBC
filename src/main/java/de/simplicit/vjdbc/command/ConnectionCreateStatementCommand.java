package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;


public class ConnectionCreateStatementCommand implements Command {

	public static final ConnectionCreateStatementCommand INSTANCE = new ConnectionCreateStatementCommand();
	
	private ConnectionCreateStatementCommand() {
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	}

	@Override
	public Object execute(Object target, ConnectionContext ctx) throws SQLException {
		return ((Connection) target).createStatement();
	}

}
