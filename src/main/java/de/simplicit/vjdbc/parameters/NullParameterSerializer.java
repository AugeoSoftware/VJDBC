package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NullParameterSerializer extends Serializer<NullParameter> {

	@Override
	public void write(Kryo kryo, Output output, NullParameter object) {
		output.writeInt(object.getSqlType());
		kryo.writeObjectOrNull(output, object.getTypeName(), String.class);
	}

	@Override
	public NullParameter read(Kryo kryo, Input input, Class<NullParameter> type) {
		return new NullParameter(input.readInt(), kryo.readObjectOrNull(input, String.class));
	}

}
