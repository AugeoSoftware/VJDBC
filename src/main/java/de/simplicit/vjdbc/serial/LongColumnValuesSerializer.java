package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class LongColumnValuesSerializer extends Serializer<LongColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, LongColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		long[] values = (long[]) object.getValues();
		for (int i=0; i<size; i++){
			output.writeLong(values[i]);
		}
		size = (size >> 5) + 1;
		int[] nullFlags = object.getNullFlags();
		for (int i=0; i<size; i++){
			output.writeInt(nullFlags[i]);
		}
	}

	@Override
	public LongColumnValues read(Kryo kryo, Input input, Class<LongColumnValues> type) {
		int size = input.readInt();
		long[] values = new long[size];
		for (int i=0; i<size; i++){
			values[i] = input.readLong();
		}
		size = (size >> 5) + 1;
		int[] nullFlags = new int[size];
		for (int i=0; i<size; i++){
			nullFlags[i] = input.readInt();
		}		
		return new LongColumnValues(values, nullFlags);
	}

}
