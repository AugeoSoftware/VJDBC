package de.simplicit.vjdbc.serial;

import java.math.BigDecimal;
import java.sql.SQLException;

public class LongColumnValues extends ColumnValues {

	private long[] values = null;
	private int[] nullFlags = null;
	
	LongColumnValues(long[] values, int[] nullFlags) {
		super(Long.TYPE);
		this.values = values;
		this.nullFlags = nullFlags;
		this.size = values.length;
	}

	public LongColumnValues(int initialSize) {
		super(Long.TYPE);
		values = new long[initialSize];
		nullFlags = new int[(initialSize >> 5) + 1];
	}

	private final void ensureCapacity(int minCapacity) {
		if (size<minCapacity){
			size = minCapacity;
		}
		int oldCapacity = values.length;
		if (minCapacity > oldCapacity) {
			int newCapacity = (oldCapacity * 3) / 2 + 1;
			if (newCapacity < minCapacity) {
				newCapacity = minCapacity;
			}

			long[] tmpValues = values;
			values = new long[newCapacity];
			System.arraycopy(tmpValues, 0, values, 0, oldCapacity);

			int[] tmpNullFlags = nullFlags;
			nullFlags = new int[(newCapacity >> 5) + 1];
			System.arraycopy(tmpNullFlags, 0, nullFlags, 0, oldCapacity);
		}
	}

	@Override
	final void setIsNull(int index) {
		int i = index >> 5;
		int m = 1 << (index & 31);
		nullFlags[i] = nullFlags[i] | m;
	}

	@Override
	final boolean isNull(int index) {
		int i = index >> 5;
		int m = 1 << (index & 31);
		return (nullFlags[i] & m)!=0;
	}

	@Override
	final void setLong(int index, long value) {
		values[index] = value;
	}

	@Override
	final long getLong(int index) {
		return values[index];
	}

	@Override
	final Object getObject(int index) {
		return Long.valueOf(getLong(index));
	}
	
	@Override
	final Object getValues() {
		return values;
	}

	final int[] getNullFlags(){
		return nullFlags;
	}

	@Override
	final float getFloat(int index) throws SQLException {
		return values[index];
	}

	@Override
	final double getDouble(int index) throws SQLException {
		return values[index];
	}

	@Override
	final String getString(int index) throws SQLException {
		return Long.toString(values[index]);
	}

	@Override
	final BigDecimal getBigDecimal(int index) throws SQLException {
		return new BigDecimal(values[index]);
	}

}
