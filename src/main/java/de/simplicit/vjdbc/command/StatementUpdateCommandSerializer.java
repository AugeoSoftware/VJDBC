package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StatementUpdateCommandSerializer extends Serializer<StatementUpdateCommand> {

	@Override
	public void write(Kryo kryo, Output output, StatementUpdateCommand object) {
		kryo.writeObjectOrNull(output, object.getValue(), String.class);
	}

	@Override
	public StatementUpdateCommand read(Kryo kryo, Input input, Class<StatementUpdateCommand> type) {
		return new StatementUpdateCommand(kryo.readObjectOrNull(input, String.class));
	}

}
