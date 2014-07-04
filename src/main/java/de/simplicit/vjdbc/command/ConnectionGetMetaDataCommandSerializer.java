package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ConnectionGetMetaDataCommandSerializer extends Serializer<ConnectionGetMetaDataCommand> {

	@Override
	public void write(Kryo kryo, Output output, ConnectionGetMetaDataCommand object) {
	}

	@Override
	public ConnectionGetMetaDataCommand read(Kryo kryo, Input input, Class<ConnectionGetMetaDataCommand> type) {
		return ConnectionGetMetaDataCommand.INSTANCE;
	}

}
