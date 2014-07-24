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
	final void setIsNull(int index) {
		values[index] = null;
	}

	@Override
	final boolean isNull(int index) {
		return values[index]==null;
	}

	@Override
	final void setObject(int index, Object value) {
		assert value==null || componentType.isAssignableFrom(value.getClass());
		values[index] = value;
	}

	@Override
	final Object getObject(int index) {
		return values[index];
	}

	@Override
	final Object getValues() {
		return values;
	}
}
