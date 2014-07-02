package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NStringParameterSerializer extends Serializer<NStringParameter> {

	@Override
	public void write(Kryo kryo, Output output, NStringParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), String.class);
	}

	@Override
	public NStringParameter read(Kryo kryo, Input input, Class<NStringParameter> type) {
		return new NStringParameter(kryo.readObjectOrNull(input, String.class));
	}

}
