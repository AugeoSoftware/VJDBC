package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.parameters.PreparedStatementParameter;

public class PreparedStatementUpdateCommandSerializer extends Serializer<PreparedStatementUpdateCommand> {

	@Override
	public void write(Kryo kryo, Output output, PreparedStatementUpdateCommand object) {
		kryo.writeObjectOrNull(output, object.getParams(), PreparedStatementParameter[].class);
	}

	@Override
	public PreparedStatementUpdateCommand read(Kryo kryo, Input input, Class<PreparedStatementUpdateCommand> type) {
		return new PreparedStatementUpdateCommand(kryo.readObjectOrNull(input, PreparedStatementParameter[].class));
	}

}
