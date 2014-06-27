package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CallingContextSerializer extends Serializer<CallingContext>{

	@Override
	public void write(Kryo kryo, Output output, CallingContext object) {
		kryo.writeObjectOrNull(output, object.getStackTrace(), String.class);
	}

	@Override
	public CallingContext read(Kryo kryo, Input input, Class<CallingContext> type) {
		String stackTrace = kryo.readObjectOrNull(input, String.class);
		return new CallingContext(stackTrace);
	}

}
