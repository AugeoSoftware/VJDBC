package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ObjectColumnValuesSerializer extends Serializer<ObjectColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, ObjectColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		Class componentType = object.getComponentType();
		kryo.writeClass(output, componentType);
		Object[] values = (Object[]) object.getValues();
		for (int i=0; i<size; i++){
			kryo.writeObjectOrNull(output, values[i], componentType);
		}
	}

	@Override
	public ObjectColumnValues read(Kryo kryo, Input input, Class<ObjectColumnValues> type) {
		int size = input.readInt();
		Class componentType = kryo.readClass(input).getType();
		Object[] values = new Object[size];
		for (int i=0; i<size; i++){
			values[i] = kryo.readObjectOrNull(input, componentType);
		}
		return new ObjectColumnValues(componentType, values);
	}

}
