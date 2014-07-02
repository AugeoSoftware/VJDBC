package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.serial.SerialSQLXML;

public class SQLXMLParameterSerializer extends Serializer<SQLXMLParameter> {

	@Override
	public void write(Kryo kryo, Output output, SQLXMLParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), SerialSQLXML.class);
	}

	@Override
	public SQLXMLParameter read(Kryo kryo, Input input, Class<SQLXMLParameter> type) {
		return new SQLXMLParameter(kryo.readObjectOrNull(input, SerialSQLXML.class));
	}

}
