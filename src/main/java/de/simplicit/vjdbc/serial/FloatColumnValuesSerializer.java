package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FloatColumnValuesSerializer extends Serializer<FloatColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, FloatColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		float[] values = (float[]) object.getValues();
		for (int i=0; i<size; i++){
			output.writeFloat(values[i]);
		}
		size = (size >> 5) + 1;
		int[] nullFlags = object.getNullFlags();
		for (int i=0; i<size; i++){
			output.writeInt(nullFlags[i]);
		}
	}

	@Override
	public FloatColumnValues read(Kryo kryo, Input input, Class<FloatColumnValues> type) {
		int size = input.readInt();
		float[] values = new float[size];
		for (int i=0; i<size; i++){
			values[i] = input.readFloat();
		}
		size = (size >> 5) + 1;
		int[] nullFlags = new int[size];
		for (int i=0; i<size; i++){
			nullFlags[i] = input.readInt();
		}		
		return new FloatColumnValues(values, nullFlags);
	}

}
