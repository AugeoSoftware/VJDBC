package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NextRowPacketCommandSerializer extends Serializer<NextRowPacketCommand> {

	@Override
	public void write(Kryo kryo, Output output, NextRowPacketCommand object) {
	}

	@Override
	public NextRowPacketCommand read(Kryo kryo, Input input, Class<NextRowPacketCommand> type) {
		return NextRowPacketCommand.INSTANCE;
	}

}
