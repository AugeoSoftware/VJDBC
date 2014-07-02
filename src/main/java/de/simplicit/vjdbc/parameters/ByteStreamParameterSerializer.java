package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ByteStreamParameterSerializer extends Serializer<ByteStreamParameter> {

	@Override
	public void write(Kryo kryo, Output output, ByteStreamParameter object) {
		output.writeInt(object.getType());
		kryo.writeObjectOrNull(output, object.getValue(), byte[].class);
		output.writeLong(object.getLength());
	}

	@Override
	public ByteStreamParameter read(Kryo kryo, Input input, Class<ByteStreamParameter> type) {
		return new ByteStreamParameter(input.readInt(), kryo.readObjectOrNull(input, byte[].class), input.readLong());
	}

}
