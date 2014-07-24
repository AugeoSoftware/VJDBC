package de.simplicit.vjdbc.serial;

import java.math.BigDecimal;
import java.sql.SQLException;

public class ByteColumnValues extends ColumnValues {

	private byte[] values;
	private int[] nullFlags = null;

	ByteColumnValues(byte[] values, int[] nullFlags) {
		super(Byte.TYPE);
		this.values = values;
		this.nullFlags = nullFlags;
		this.size = values.length;
	}
	
	ByteColumnValues(int initialSize) {
		super(Byte.TYPE);
		values = new byte[initialSize];
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

			byte[] tmpValues = values;
			values = new byte[newCapacity];
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
	final void setByte(int index, byte value) {
		values[index] = value;
	}

	@Override
	final byte getByte(int index) {
		return values[index];
	}

	@Override
	final Object getObject(int index) {
		return Byte.valueOf(getByte(index));
	}
	
	@Override
	final Object getValues() {
		return values;
	}

	final int[] getNullFlags(){
		return nullFlags;
	}
	
	@Override
	final short getShort(int index) throws SQLException {
		return values[index];
	}

	@Override
	final int getInt(int index) throws SQLException {
		return values[index];
	}

	@Override
	final long getLong(int index) throws SQLException {
		return values[index];
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
		return Short.toString(values[index]);
	}

	@Override
	final BigDecimal getBigDecimal(int index) throws SQLException {
		return new BigDecimal(values[index]);
	}
	
}
