package de.simplicit.vjdbc.parameters;

import java.sql.Date;
import java.util.Calendar;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DateParameterSerializer extends Serializer<DateParameter> {

	@Override
	public void write(Kryo kryo, Output output, DateParameter object) {
		kryo.writeObjectOrNull(output, object.getValue(), Date.class);
		kryo.writeObjectOrNull(output, object.getCalendar(), Calendar.class);
	}

	@Override
	public DateParameter read(Kryo kryo, Input input, Class<DateParameter> type) {
		return new DateParameter(kryo.readObjectOrNull(input, Date.class), kryo.readObjectOrNull(input, Calendar.class));
	}

}
