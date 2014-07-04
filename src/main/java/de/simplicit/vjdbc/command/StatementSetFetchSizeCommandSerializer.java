package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StatementSetFetchSizeCommandSerializer extends Serializer<StatementSetFetchSizeCommand> {

	@Override
	public void write(Kryo kryo, Output output, StatementSetFetchSizeCommand object) {
		output.writeInt(object.getValue());
	}

	@Override
	public StatementSetFetchSizeCommand read(Kryo kryo, Input input, Class<StatementSetFetchSizeCommand> type) {
		return new StatementSetFetchSizeCommand(input.readInt());
	}

}
