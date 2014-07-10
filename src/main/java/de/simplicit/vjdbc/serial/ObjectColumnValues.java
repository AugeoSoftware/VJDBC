package de.simplicit.vjdbc.serial;


public class ObjectColumnValues extends ColumnValues {

	private Object[] values;
	
	ObjectColumnValues(Class componentType, Object[] values) {
		super(componentType);
		this.values = values;
		this.size = values.length;
	}


	public ObjectColumnValues(Class componentType, int initialSize) {
		super(componentType);
		values = new Object[initialSize];
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
      	    Object[] tmpValues = values;
      	    values = new Object[newCapacity];
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
		values[index] = value;
	}

	@Override
	Object getObject(int index) {
		return values[index];
	}

	@Override
	Object getValues() {
		return values;
	}
}
