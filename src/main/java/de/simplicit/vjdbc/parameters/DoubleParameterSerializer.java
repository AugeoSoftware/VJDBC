package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DoubleParameterSerializer extends Serializer<DoubleParameter> {

	@Override
	public void write(Kryo kryo, Output output, DoubleParameter object) {
		output.writeDouble(object.getValue());
	}

	@Override
	public DoubleParameter read(Kryo kryo, Input input, Class<DoubleParameter> type) {
		return new DoubleParameter(input.readDouble());
	}

}
