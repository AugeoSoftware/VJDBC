package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CharStreamParameterSerializer extends Serializer<CharStreamParameter> {

	@Override
	public void write(Kryo kryo, Output output, CharStreamParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), char[].class);
	}

	@Override
	public CharStreamParameter read(Kryo kryo, Input input, Class<CharStreamParameter> type) {
		return new CharStreamParameter(kryo.readObjectOrNull(input, char[].class));
	}

}
