package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class IntegerColumnValuesSerializer extends Serializer<IntegerColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, IntegerColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		int[] values = (int[]) object.getValues();
		for (int i=0; i<size; i++){
			output.writeInt(values[i]);
		}
		size = (size >> 5) + 1;
		int[] nullFlags = object.getNullFlags();
		for (int i=0; i<size; i++){
			output.writeInt(nullFlags[i]);
		}
	}

	@Override
	public IntegerColumnValues read(Kryo kryo, Input input, Class<IntegerColumnValues> type) {
		int size = input.readInt();
		int[] values = new int[size];
		for (int i=0; i<size; i++){
			values[i] = input.readInt();
		}
		size = (size >> 5) + 1;
		int[] nullFlags = new int[size];
		for (int i=0; i<size; i++){
			nullFlags[i] = input.readInt();
		}		
		return new IntegerColumnValues(values, nullFlags);
	}

}
