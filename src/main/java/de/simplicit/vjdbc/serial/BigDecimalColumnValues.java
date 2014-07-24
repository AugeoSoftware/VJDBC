package de.simplicit.vjdbc.serial;

import java.math.BigDecimal;
import java.sql.SQLException;

public class BigDecimalColumnValues extends ColumnValues {

	private BigDecimal[] values;

	BigDecimalColumnValues(BigDecimal[] values) {
		super(BigDecimal.class);
		this.values = values;
		this.size = values.length;
	}

	BigDecimalColumnValues(int initialSize) {
		super(BigDecimal.class);
		values = new BigDecimal[initialSize];
	}

	private final void ensureCapacity(int minCapacity) {
		if (size < minCapacity) {
			size = minCapacity;
		}
		int oldCapacity = values.length;
		if (minCapacity > oldCapacity) {
			int newCapacity = (oldCapacity * 3) / 2 + 1;
			if (newCapacity < minCapacity) {
				newCapacity = minCapacity;
			}
			BigDecimal[] tmpValues = values;
			values = new BigDecimal[newCapacity];
			System.arraycopy(tmpValues, 0, values, 0, oldCapacity);
		}
	}

	@Override
	final void setIsNull(int index) {
		values[index] = null;
	}

	@Override
	final boolean isNull(int index) {
		return values[index] == null;
	}

	@Override
	final void setObject(int index, Object value) {
		assert value == null || componentType.isAssignableFrom(value.getClass());
		values[index] = (BigDecimal) value;
	}

	@Override
	final Object getObject(int index) {
		return values[index];
	}

	@Override
	final Object getValues() {
		return values;
	}

	@Override
	final boolean getBoolean(int index) throws SQLException {
		return BigDecimal.ONE.equals(values[index]);
	}

	@Override
	final byte getByte(int index) throws SQLException {
		if (values[index] != null) {
			return values[index].byteValue();
		}
		return 0;
	}

	@Override
	final short getShort(int index) throws SQLException {
		if (values[index] != null) {
			return values[index].shortValue();
		}
		return 0;
	}

	@Override
	final int getInt(int index) throws SQLException {
		if (values[index] != null) {
			return values[index].intValue();
		}
		return 0;
	}

	@Override
	final long getLong(int index) throws SQLException {
		if (values[index] != null) {
			return values[index].longValue();
		}
		return 0L;
	}

	@Override
	final float getFloat(int index) throws SQLException {
		if (values[index] != null) {
			return values[index].floatValue();
		}
		return 0.0f;
	}

	@Override
	final double getDouble(int index) throws SQLException {
		if (values[index] != null) {
			return values[index].doubleValue();
		}
		return 0.0;
	}

	@Override
	final String getString(int index) throws SQLException {
		if (values[index] != null) {
			return values[index].toString();
		}
		return null;
	}

	@Override
	final BigDecimal getBigDecimal(int index) throws SQLException {
		return values[index];
	}

}
