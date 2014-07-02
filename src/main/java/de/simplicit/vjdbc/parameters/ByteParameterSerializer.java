package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ByteParameterSerializer extends Serializer<ByteParameter> {

	@Override
	public void write(Kryo kryo, Output output, ByteParameter object) {
		output.writeByte(object.getValue());
	}

	@Override
	public ByteParameter read(Kryo kryo, Input input, Class<ByteParameter> type) {
		return new ByteParameter(input.readByte());
	}

}
