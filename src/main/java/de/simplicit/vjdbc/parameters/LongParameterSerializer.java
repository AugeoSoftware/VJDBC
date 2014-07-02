package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class LongParameterSerializer extends Serializer<LongParameter> {

	@Override
	public void write(Kryo kryo, Output output, LongParameter object) {
		output.writeLong(object.getValue());
	}

	@Override
	public LongParameter read(Kryo kryo, Input input, Class<LongParameter> type) {
		return new LongParameter(input.readLong());
	}

}
