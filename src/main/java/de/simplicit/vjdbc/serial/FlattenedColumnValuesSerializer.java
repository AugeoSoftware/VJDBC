package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FlattenedColumnValuesSerializer extends Serializer<FlattenedColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, FlattenedColumnValues object) {
		kryo.writeClassAndObject(output, object.getArrayOfValues());
		kryo.writeObjectOrNull(output, object.getNullFlags(), boolean[].class);
	}

	@Override
	public FlattenedColumnValues read(Kryo kryo, Input input, Class<FlattenedColumnValues> type) {				
		Object arrayObject = kryo.readClassAndObject(input);
		boolean[] nullFlags = kryo.readObjectOrNull(input, boolean[].class);
		return new FlattenedColumnValues(arrayObject, nullFlags);
	}

}
