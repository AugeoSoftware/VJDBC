package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DoubleColumnValuesSerializer extends Serializer<DoubleColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, DoubleColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		double[] values = (double[]) object.getValues();
		for (int i=0; i<size; i++){
			output.writeDouble(values[i]);
		}
		size = (size >> 5) + 1;
		int[] nullFlags = object.getNullFlags();
		for (int i=0; i<size; i++){
			output.writeInt(nullFlags[i]);
		}
	}

	@Override
	public DoubleColumnValues read(Kryo kryo, Input input, Class<DoubleColumnValues> type) {
		int size = input.readInt();
		double[] values = new double[size];
		for (int i=0; i<size; i++){
			values[i] = input.readDouble();
		}
		size = (size >> 5) + 1;
		int[] nullFlags = new int[size];
		for (int i=0; i<size; i++){
			nullFlags[i] = input.readInt();
		}		
		return new DoubleColumnValues(values, nullFlags);
	}

}
