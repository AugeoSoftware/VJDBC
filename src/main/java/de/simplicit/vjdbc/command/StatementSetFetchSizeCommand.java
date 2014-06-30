package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.sql.Statement;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StatementSetFetchSizeCommand implements Command, KryoSerializable {

	private int _value;
	
	public StatementSetFetchSizeCommand() {
	}

	public StatementSetFetchSizeCommand(int value) {
		super();
		this._value = value;
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
	public void write(Kryo kryo, Output output) {
		output.writeInt(_value);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_value = input.readInt();
	}

	@Override
	public Object execute(Object target, ConnectionContext ctx) throws SQLException {
		((Statement)target).setFetchSize(_value);
		return null;
	}

}
