package de.simplicit.vjdbc.parameters;

import java.math.BigDecimal;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BigDecimalParameterSerializer extends Serializer<BigDecimalParameter> {

	@Override
	public void write(Kryo kryo, Output output, BigDecimalParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), BigDecimal.class);
	}

	@Override
	public BigDecimalParameter read(Kryo kryo, Input input, Class<BigDecimalParameter> type) {
		return new BigDecimalParameter(kryo.readObjectOrNull(input, BigDecimal.class));
	}

}
