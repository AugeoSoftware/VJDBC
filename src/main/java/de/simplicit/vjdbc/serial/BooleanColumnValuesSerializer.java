package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BooleanColumnValuesSerializer extends Serializer<BooleanColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, BooleanColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		size = (size >> 5) + 1;
		int[] values = (int[]) object.getValues();
		for (int i=0; i<size; i++){
			output.writeInt(values[i]);
		}
		int[] nullFlags = object.getNullFlags();
		for (int i=0; i<size; i++){
			output.writeInt(nullFlags[i]);
		}
	}

	@Override
	public BooleanColumnValues read(Kryo kryo, Input input, Class<BooleanColumnValues> type) {
		int size = input.readInt();
		int length = (size >> 5) + 1;
		
		int[] values = new int[length];
		for (int i=0; i<length; i++){
			values[i] = input.readInt();
		}
		int[] nullFlags = new int[length];
		for (int i=0; i<length; i++){
			nullFlags[i] = input.readInt();
		}		
		return new BooleanColumnValues(values, nullFlags, size);
	}

}
