package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ConnectionCreateStatementCommandSerializer extends Serializer<ConnectionCreateStatementCommand> {

	@Override
	public void write(Kryo kryo, Output output, ConnectionCreateStatementCommand object) {
	}

	@Override
	public ConnectionCreateStatementCommand read(Kryo kryo, Input input, Class<ConnectionCreateStatementCommand> type) {
		return ConnectionCreateStatementCommand.INSTANCE;
	}

}
