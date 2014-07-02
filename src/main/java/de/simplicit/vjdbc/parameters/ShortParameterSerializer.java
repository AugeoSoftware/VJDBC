package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ShortParameterSerializer extends Serializer<ShortParameter> {

	@Override
	public void write(Kryo kryo, Output output, ShortParameter object) {
		output.writeShort(object.getValue());
	}

	@Override
	public ShortParameter read(Kryo kryo, Input input, Class<ShortParameter> type) {
		return new ShortParameter(input.readShort());
	}

}
