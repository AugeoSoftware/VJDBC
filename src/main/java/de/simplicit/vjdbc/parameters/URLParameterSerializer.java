package de.simplicit.vjdbc.parameters;

import java.net.MalformedURLException;
import java.net.URL;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class URLParameterSerializer extends Serializer<URLParameter> {

	@Override
	public void write(Kryo kryo, Output output, URLParameter object) {
		kryo.writeObjectOrNull(output, object.getValue().toString(), String.class);
	}

	@Override
	public URLParameter read(Kryo kryo, Input input, Class<URLParameter> type) {
		try {
			return new URLParameter(new URL(kryo.readObjectOrNull(input, String.class)));
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

}
