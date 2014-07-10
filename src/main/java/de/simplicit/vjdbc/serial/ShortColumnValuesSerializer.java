package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ShortColumnValuesSerializer extends Serializer<ShortColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, ShortColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		short[] values = (short[]) object.getValues();
		for (int i=0; i<size; i++){
			output.writeShort(values[i]);
		}
		size = (size >> 5) + 1;
		int[] nullFlags = object.getNullFlags();
		for (int i=0; i<size; i++){
			output.writeInt(nullFlags[i]);
		}
	}

	@Override
	public ShortColumnValues read(Kryo kryo, Input input, Class<ShortColumnValues> type) {
		int size = input.readInt();
		short[] values = new short[size];
		for (int i=0; i<size; i++){
			values[i] = input.readShort();
		}
		size = (size >> 5) + 1;
		int[] nullFlags = new int[size];
		for (int i=0; i<size; i++){
			nullFlags[i] = input.readInt();
		}		
		return new ShortColumnValues(values, nullFlags);
	}

}
