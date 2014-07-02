package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.serial.SerialClob;

public class ClobParameterSerializer extends Serializer<ClobParameter> {

	@Override
	public void write(Kryo kryo, Output output, ClobParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), SerialClob.class);
	}

	@Override
	public ClobParameter read(Kryo kryo, Input input, Class<ClobParameter> type) {
		return new ClobParameter(kryo.readObjectOrNull(input, SerialClob.class));
	}

}
