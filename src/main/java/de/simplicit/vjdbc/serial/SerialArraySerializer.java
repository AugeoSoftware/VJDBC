package de.simplicit.vjdbc.serial;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerialArraySerializer extends Serializer<SerialArray>{

	@Override
	public void write(Kryo kryo, Output output, SerialArray object) {
		output.writeInt(object.getBaseType());
		kryo.writeObjectOrNull(output, object.getBaseTypeName(), String.class);
		kryo.writeClassAndObject(output, object.getArray());
	}

	@Override
	public SerialArray read(Kryo kryo, Input input, Class<SerialArray> type) {
		int baseType = input.readInt();
		String baseTypeName = kryo.readObjectOrNull(input, String.class);
		Object array = kryo.readClassAndObject(input);
		return new SerialArray(baseType, baseTypeName, array);
	}

}
