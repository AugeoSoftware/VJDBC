package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerialRefSerializer extends Serializer<SerialRef> {

	@Override
	public void write(Kryo kryo, Output output, SerialRef object) {
		kryo.writeObjectOrNull(output, object.getBaseTypeName(), String.class);
		kryo.writeClassAndObject(output, object.getObject());
	}

	@Override
	public SerialRef read(Kryo kryo, Input input, Class<SerialRef> type) {
		return new SerialRef(kryo.readObjectOrNull(input, String.class), kryo.readClassAndObject(input));
	}

}
