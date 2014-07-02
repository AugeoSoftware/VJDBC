package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FloatParameterSerializer extends Serializer<FloatParameter> {

	@Override
	public void write(Kryo kryo, Output output, FloatParameter object) {
		output.writeFloat(object.getValue());
	}

	@Override
	public FloatParameter read(Kryo kryo, Input input, Class<FloatParameter> type) {
		return new FloatParameter(input.readFloat());
	}

}
