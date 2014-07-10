package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ByteColumnValuesSerializer extends Serializer<ByteColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, ByteColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		byte[] values = (byte[]) object.getValues();
		for (int i=0; i<size; i++){
			output.writeByte(values[i]);
		}
		size = (size >> 5) + 1;
		int[] nullFlags = object.getNullFlags();
		for (int i=0; i<size; i++){
			output.writeInt(nullFlags[i]);
		}
	}

	@Override
	public ByteColumnValues read(Kryo kryo, Input input, Class<ByteColumnValues> type) {
		int size = input.readInt();
		byte[] values = new byte[size];
		for (int i=0; i<size; i++){
			values[i] = input.readByte();
		}
		size = (size >> 5) + 1;
		int[] nullFlags = new int[size];
		for (int i=0; i<size; i++){
			nullFlags[i] = input.readInt();
		}		
		return new ByteColumnValues(values, nullFlags);
	}

}
