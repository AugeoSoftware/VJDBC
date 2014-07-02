package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ByteArrayParameterSerializer extends Serializer<ByteArrayParameter> {

	@Override
	public void write(Kryo kryo, Output output, ByteArrayParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), byte[].class);
	}

	@Override
	public ByteArrayParameter read(Kryo kryo, Input input, Class<ByteArrayParameter> type) {
		return new ByteArrayParameter(kryo.readObjectOrNull(input, byte[].class));
	}

}
