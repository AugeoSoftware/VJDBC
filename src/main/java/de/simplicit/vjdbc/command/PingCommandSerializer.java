package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PingCommandSerializer extends Serializer<PingCommand> {

	@Override
	public void write(Kryo kryo, Output output, PingCommand object) {
	}

	@Override
	public PingCommand read(Kryo kryo, Input input, Class<PingCommand> type) {
		return PingCommand.INSTANCE;
	}

}
