package de.simplicit.vjdbc.serial;

import java.math.BigDecimal;
import java.sql.SQLException;

public class IntegerColumnValues extends ColumnValues {

	private int[] values = null;
	private int[] nullFlags = null;

	public IntegerColumnValues(int initialSize) {
		super(Integer.TYPE);
		values = new int[initialSize];
		nullFlags = new int[(initialSize >> 5) + 1];
	}

	IntegerColumnValues(int[] values, int[] nullFlags) {
		super(Integer.TYPE);
		this.values = values;
		this.nullFlags = nullFlags;
		this.size = values.length;		
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

			int[] tmpValues = values;
			values = new int[newCapacity];
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
	void setInt(int index, int value) {
		ensureCapacity(index+1);
		values[index] = value;
	}

	@Override
	int getInt(int index) {
		return values[index];
	}

	@Override
	Object getObject(int index) {
		return Integer.valueOf(getInt(index));
	}
	
	@Override
	Object getValues() {
		return values;
	}

	int[] getNullFlags() {
		return nullFlags;
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
		return Integer.toString(values[index]);
	}

	@Override
	BigDecimal getBigDecimal(int index) throws SQLException {
		return new BigDecimal(values[index]);
	}

}
