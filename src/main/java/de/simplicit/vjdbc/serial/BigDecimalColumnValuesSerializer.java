package de.simplicit.vjdbc.serial;

import java.math.BigDecimal;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BigDecimalColumnValuesSerializer extends Serializer<BigDecimalColumnValues> {

	@Override
	public void write(Kryo kryo, Output output, BigDecimalColumnValues object) {
		int size = object.size();
		output.writeInt(size);
		Object[] values = (Object[]) object.getValues();
		for (int i=0; i<size; i++){
			kryo.writeObjectOrNull(output, values[i], BigDecimal.class);
		}
	}

	@Override
	public BigDecimalColumnValues read(Kryo kryo, Input input, Class<BigDecimalColumnValues> type) {
		int size = input.readInt();
		BigDecimal[] values = new BigDecimal[size];
		for (int i=0; i<size; i++){
			values[i] = kryo.readObjectOrNull(input, BigDecimal.class);
		}
		return new BigDecimalColumnValues(values);
	}

}
