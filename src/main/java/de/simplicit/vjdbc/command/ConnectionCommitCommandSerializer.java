package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ConnectionCommitCommandSerializer extends Serializer<ConnectionCommitCommand> {

	@Override
	public void write(Kryo kryo, Output output, ConnectionCommitCommand object) {
	}

	@Override
	public ConnectionCommitCommand read(Kryo kryo, Input input, Class<ConnectionCommitCommand> type) {
		return ConnectionCommitCommand.INSTANCE;
	}

}
