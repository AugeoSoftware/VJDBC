package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.serial.SerialArray;

public class ArrayParameterSerializer extends Serializer<ArrayParameter> {

	@Override
	public void write(Kryo kryo, Output output, ArrayParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), SerialArray.class);
	}

	@Override
	public ArrayParameter read(Kryo kryo, Input input, Class<ArrayParameter> type) {
		return new ArrayParameter(kryo.readObjectOrNull(input, SerialArray.class));
	}

}
