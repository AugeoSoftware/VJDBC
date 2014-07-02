package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class StringParameterSerializer extends Serializer<StringParameter> {

	@Override
	public void write(Kryo kryo, Output output, StringParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), String.class);
	}

	@Override
	public StringParameter read(Kryo kryo, Input input, Class<StringParameter> type) {
		return new StringParameter(kryo.readObjectOrNull(input, String.class));
	}

}
