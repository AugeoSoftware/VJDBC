package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.serial.SerialRowId;

public class RowIdParameterSerializer extends Serializer<RowIdParameter> {

	@Override
	public void write(Kryo kryo, Output output, RowIdParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), SerialRowId.class);
	}

	@Override
	public RowIdParameter read(Kryo kryo, Input input, Class<RowIdParameter> type) {
		return new RowIdParameter(kryo.readObjectOrNull(input, SerialRowId.class));
	}

}
