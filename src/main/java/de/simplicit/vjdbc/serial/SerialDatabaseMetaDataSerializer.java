package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerialDatabaseMetaDataSerializer extends Serializer<SerialDatabaseMetaData> {

	@Override
	public void write(Kryo kryo, Output output, SerialDatabaseMetaData object) {
		kryo.writeObject(output, object.getUIDEx());
		kryo.writeObjectOrNull(output, object.getDatabaseProductName(), String.class);
		kryo.writeObjectOrNull(output, object.getDatabaseProductVersion(), String.class);
		kryo.writeObjectOrNull(output, object.getDriverName(), String.class);
		kryo.writeObjectOrNull(output, object.getDriverVersion(), String.class);
	}

	@Override
	public SerialDatabaseMetaData read(Kryo kryo, Input input, Class<SerialDatabaseMetaData> type) {
		return new SerialDatabaseMetaData(
				kryo.readObject(input, UIDEx.class), 
				kryo.readObjectOrNull(input, String.class), 
				kryo.readObjectOrNull(input, String.class), 
				kryo.readObjectOrNull(input, String.class), 
				kryo.readObjectOrNull(input, String.class));
	}

}
