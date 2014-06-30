package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ConnectionSetAutoCommitCommand implements Command, KryoSerializable {

	private boolean _value;
	
	public ConnectionSetAutoCommitCommand() {
	}

	public ConnectionSetAutoCommitCommand(boolean value) {
		super();
		this._value = value;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(_value);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		_value = in.readBoolean();
	}

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeBoolean(_value);	
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_value = input.readBoolean();
	}

	@Override
	public Object execute(Object target, ConnectionContext ctx) throws SQLException {
		((Connection) target).setAutoCommit(_value);
		return null;
	}

}
