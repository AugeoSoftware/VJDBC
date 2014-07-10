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
		if (size<minCapacity){
			size = minCapacity;
		}
    	int oldCapacity = values.length;
    	if (minCapacity > oldCapacity) {
    		int newCapacity = (oldCapacity * 3)/2 + 1;
      	    if (newCapacity < minCapacity) {
      	        newCapacity = minCapacity;
      	    }	
      	  BigDecimal[] tmpValues = values;
      	    values = new BigDecimal[newCapacity];
      	    System.arraycopy(tmpValues, 0, values, 0, oldCapacity);
    	}
    }
	
	@Override
	void setIsNull(int index) {
		ensureCapacity(index + 1);
		values[index] = null;
	}

	@Override
	boolean isNull(int index) {
		return values[index]==null;
	}

	@Override
	void setObject(int index, Object value) {
		assert value==null || componentType.isAssignableFrom(value.getClass());
		ensureCapacity(index + 1);
		values[index] = (BigDecimal) value;
	}

	@Override
	Object getObject(int index) {
		return values[index];
	}

	@Override
	Object getValues() {
		return values;
	}

	@Override
	boolean getBoolean(int index) throws SQLException {
		return BigDecimal.ONE.equals(values[index]);
	}


	@Override
	byte getByte(int index) throws SQLException {
		if (values[index]!=null){
			return values[index].byteValue();
		} 
		return 0;
	}

	@Override
	short getShort(int index) throws SQLException {
		if (values[index]!=null){
			return values[index].shortValue();
		} 
		return 0;
	}

	@Override
	int getInt(int index) throws SQLException {
		if (values[index]!=null){
			return values[index].intValue();
		} 
		return 0;
	}

	@Override
	long getLong(int index) throws SQLException {
		if (values[index]!=null){
			return values[index].longValue();
		} 
		return 0L;
	}

	@Override
	float getFloat(int index) throws SQLException {
		if (values[index]!=null){
			return values[index].floatValue();
		} 
		return 0.0f;
	}

	@Override
	double getDouble(int index) throws SQLException {
		if (values[index]!=null){
			return values[index].doubleValue();
		} 
		return 0.0;
	}

	@Override
	String getString(int index) throws SQLException {
		if (values[index]!=null){
			return values[index].toString();
		}
		return null;
	}

	@Override
	BigDecimal getBigDecimal(int index) throws SQLException {
		return values[index];
	}

}
