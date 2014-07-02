package de.simplicit.vjdbc.parameters;

import java.sql.Time;
import java.util.Calendar;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TimeParameterSerializer extends Serializer<TimeParameter> {

	@Override
	public void write(Kryo kryo, Output output, TimeParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), Time.class);
		kryo.writeObjectOrNull(output, object.getCalendar(), Calendar.class);
	}

	@Override
	public TimeParameter read(Kryo kryo, Input input, Class<TimeParameter> type) {
		return new TimeParameter(kryo.readObjectOrNull(input, Time.class), kryo.readObjectOrNull(input, Calendar.class));
	}

}
