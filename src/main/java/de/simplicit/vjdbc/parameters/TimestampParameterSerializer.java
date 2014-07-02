package de.simplicit.vjdbc.parameters;

import java.sql.Timestamp;
import java.util.Calendar;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TimestampParameterSerializer extends Serializer<TimestampParameter> {

	@Override
	public void write(Kryo kryo, Output output, TimestampParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), Timestamp.class);
		kryo.writeObjectOrNull(output, object.getCalendar(), Calendar.class);
	}

	@Override
	public TimestampParameter read(Kryo kryo, Input input, Class<TimestampParameter> type) {
		return new TimestampParameter(kryo.readObjectOrNull(input, Timestamp.class), kryo.readObjectOrNull(input, Calendar.class));
	}

}
