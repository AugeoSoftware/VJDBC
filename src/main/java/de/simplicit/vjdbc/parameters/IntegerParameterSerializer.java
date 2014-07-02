package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class IntegerParameterSerializer extends Serializer<IntegerParameter> {

	@Override
	public void write(Kryo kryo, Output output, IntegerParameter object) {
		output.writeInt(object.getValue());
	}

	@Override
	public IntegerParameter read(Kryo kryo, Input input, Class<IntegerParameter> type) {
		return new IntegerParameter(input.readInt());
	}

}
