package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerialRowIdSerializer extends Serializer<SerialRowId> {

	@Override
	public void write(Kryo kryo, Output output, SerialRowId object) {
		kryo.writeObjectOrNull(output, object.getBytes(), byte[].class);
		kryo.writeObjectOrNull(output, object.toString(), String.class);
		output.writeInt(object.hashCode());
	}

	@Override
	public SerialRowId read(Kryo kryo, Input input, Class<SerialRowId> type) {
		return new SerialRowId(kryo.readObjectOrNull(input, byte[].class),
				kryo.readObjectOrNull(input, String.class), 
				input.readInt());
	}

}
