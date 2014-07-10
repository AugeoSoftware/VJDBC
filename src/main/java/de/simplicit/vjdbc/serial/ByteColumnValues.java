package de.simplicit.vjdbc.serial;

import java.math.BigDecimal;
import java.sql.SQLException;

public class ByteColumnValues extends ColumnValues {

	private byte[] values;
	private int[] nullFlags = null;
	private int size = 0;

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
	void setIsNull(int index) {
		ensureCapacity(index + 1);
		int i = index >> 5;
		int m = 1 << (index & 31);
		nullFlags[i] = nullFlags[i] | m;
	}

	@Override
	boolean isNull(int index) {
		int i = index >> 5;
		int m = 1 << (index & 31);
		return (nullFlags[i] & m)!=0;
	}

	@Override
	void setByte(int index, byte value) {
		ensureCapacity(index+1);
		values[index] = value;
	}

	@Override
	byte getByte(int index) {
		return values[index];
	}

	@Override
	Object getObject(int index) {
		return Byte.valueOf(getByte(index));
	}
	
	@Override
	Object getValues() {
		return values;
	}

	int[] getNullFlags(){
		return nullFlags;
	}
	
	@Override
	short getShort(int index) throws SQLException {
		return values[index];
	}

	@Override
	int getInt(int index) throws SQLException {
		return values[index];
	}

	@Override
	long getLong(int index) throws SQLException {
		return values[index];
	}

	@Override
	float getFloat(int index) throws SQLException {
		return values[index];
	}

	@Override
	double getDouble(int index) throws SQLException {
		return values[index];
	}

	@Override
	String getString(int index) throws SQLException {
		return Short.toString(values[index]);
	}

	@Override
	BigDecimal getBigDecimal(int index) throws SQLException {
		return new BigDecimal(values[index]);
	}
	
}
