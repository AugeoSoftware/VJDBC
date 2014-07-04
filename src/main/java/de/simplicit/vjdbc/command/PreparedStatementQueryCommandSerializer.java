package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.parameters.PreparedStatementParameter;

public class PreparedStatementQueryCommandSerializer extends Serializer<PreparedStatementQueryCommand> {

	@Override
	public void write(Kryo kryo, Output output, PreparedStatementQueryCommand object) {
		kryo.writeObjectOrNull(output, object.getParams(), PreparedStatementParameter[].class);
		output.writeInt(object.getResultSetType());

	}

	@Override
	public PreparedStatementQueryCommand read(Kryo kryo, Input input, Class<PreparedStatementQueryCommand> type) {
		return new PreparedStatementQueryCommand(kryo.readObjectOrNull(input, PreparedStatementParameter[].class), input.readInt());
	}

}
