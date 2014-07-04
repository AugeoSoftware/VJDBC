package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementSetFetchSizeCommand implements Command {

	private int _value;
	
	public StatementSetFetchSizeCommand() {
	}

	public StatementSetFetchSizeCommand(int value) {
		this._value = value;
	}

	public int getValue() {
		return _value;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(_value);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		_value = in.readInt();
	}

	@Override
	public Object execute(Object target, ConnectionContext ctx) throws SQLException {
		((Statement)target).setFetchSize(_value);
		return null;
	}

}
