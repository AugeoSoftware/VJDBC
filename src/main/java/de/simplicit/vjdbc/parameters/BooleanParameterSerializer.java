package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BooleanParameterSerializer extends Serializer<BooleanParameter> {

	@Override
	public void write(Kryo kryo, Output output, BooleanParameter object) {
		output.writeBoolean(object.getValue());
	}

	@Override
	public BooleanParameter read(Kryo kryo, Input input, Class<BooleanParameter> type) {
		return new BooleanParameter(input.readBoolean());
	}

}
