package de.simplicit.vjdbc.serial;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public abstract class ColumnValues {
	protected final Class componentType;
	protected int size = 0;

	ColumnValues(Class componentType) {
		this.componentType = componentType;
	}

	void setIsNull(int index) {
		throw new UnsupportedOperationException();
	}

	boolean isNull(int index) {
		throw new UnsupportedOperationException();
	}

	void setBoolean(int index, boolean value) {
		throw new UnsupportedOperationException();
	}

	boolean getBoolean(int index) throws SQLException {
		
		throw new SQLException("Can not convert value "+getString(index)+" of type "+componentType.getName()+" to boolean");
	}

	void setByte(int index, byte value) {
		throw new UnsupportedOperationException();
	}

	byte getByte(int index) throws SQLException {
		throw new SQLException("Can not convert "+getString(index)+" of type "+componentType.getName()+" to byte");
	}

	void setShort(int index, short value) {
		throw new UnsupportedOperationException();
	}

	short getShort(int index) throws SQLException {
		throw new SQLException("Can not convert "+getString(index)+" of type "+componentType.getName()+" to short");
	}

	void setInt(int index, int value) {
		throw new UnsupportedOperationException();
	}

	int getInt(int index) throws SQLException {
		throw new SQLException("Can not convert "+getString(index)+" of type "+componentType.getName()+" to int");
	}

	void setLong(int index, long value) {
		throw new UnsupportedOperationException();
	}

	long getLong(int index) throws SQLException {
		throw new SQLException("Can not convert "+getString(index)+" of type "+componentType.getName()+" to long");
	}

	void setFloat(int index, float value) {
		throw new UnsupportedOperationException();
	}

	float getFloat(int index) throws SQLException {
		throw new SQLException("Can not convert "+getString(index)+" of type "+componentType.getName()+" to float");
	}

	void setDouble(int index, double value) {
		throw new UnsupportedOperationException();
	}

	double getDouble(int index) throws SQLException {
		throw new SQLException("Can not convert "+getString(index)+" of type "+componentType.getName()+" to double");
	}

	void setObject(int index, Object value) {
		throw new UnsupportedOperationException();
	}

	abstract Object getObject(int index) throws SQLException ;
	
	abstract Object getValues();
	
	int size(){
		return size;
	}

	Class getComponentType() {
		return componentType;
	}
	
	String getString(int index) throws SQLException {
		Object object = getObject(index);
		if (object!=null){
			return object.toString();
		}
		return null;
	}
	
	BigDecimal getBigDecimal(int index) throws SQLException {
		Object value = getObject(index);
		if (value instanceof BigDecimal){
			return (BigDecimal) value;
		} else if (value instanceof Number){
			return BigDecimal.valueOf(((Number) value).doubleValue());
		} else if (value instanceof Boolean){
			return ((Boolean) value).booleanValue()?BigDecimal.ONE:BigDecimal.ZERO;
		} else if (value instanceof String){
			try {
				return new BigDecimal((String)value);
			} catch (NumberFormatException e){				
				throw new SQLException("Can not convert value "+value+" to BigDecimal", e);
			}
		}
		throw new SQLException("Can't convert type to BigDecimal: " + value.getClass());		
	}
	
	Date getDate(int index) throws SQLException{
		Object value = getObject(index);
		if (value==null){
			return null;
		}
		if (value instanceof Date){
			return (Date) value;
		} else if (value instanceof Time){
			return getCleanDate(((Time) value).getTime());
		} else if (value instanceof Timestamp){
			return getCleanDate(((Timestamp) value).getTime());
		}
		throw new SQLException("Can not convert "+value.getClass()+" to java.sql.Date");
	}
	
	Time getTime(int index) throws SQLException {
		Object value = getObject(index);
		if (value==null){
			return null;
		}
		if (value instanceof Time){
			return (Time) value;
		} else if (value instanceof Date){
			return getCleanTime(((Date) value).getTime());
		} else if (value instanceof Timestamp){
			return getCleanTime(((Timestamp) value).getTime());
		}
		throw new SQLException("Can not convert "+value.getClass()+" to java.sql.Time");		
	}
	
	Timestamp getTimestamp(int index) throws SQLException{
		Object value = getObject(index);
		if (value==null){
			return null;
		}
		if (value instanceof Timestamp){
			return (Timestamp) value;
		} else if (value instanceof Date){
			return new Timestamp(((Date) value).getTime());
		} else if (value instanceof Time){
			return new Timestamp(((Time) value).getTime());
		}
		throw new SQLException("Can not convert "+value.getClass()+" to java.sql.Timestamp");			
	}
	
    private Date getCleanDate(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    private Time getCleanTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MILLISECOND, 0);
        return new Time(cal.getTimeInMillis());
    }
}
