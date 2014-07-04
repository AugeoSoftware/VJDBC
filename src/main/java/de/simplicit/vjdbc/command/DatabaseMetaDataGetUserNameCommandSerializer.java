package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DatabaseMetaDataGetUserNameCommandSerializer extends Serializer<DatabaseMetaDataGetUserNameCommand> {

	@Override
	public void write(Kryo kryo, Output output, DatabaseMetaDataGetUserNameCommand object) {
	}

	@Override
	public DatabaseMetaDataGetUserNameCommand read(Kryo kryo, Input input,
			Class<DatabaseMetaDataGetUserNameCommand> type) {
		return DatabaseMetaDataGetUserNameCommand.INSTANCE;
	}

}
