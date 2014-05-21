//VJDBC - Virtual JDBC
//Written by Michael Link
//Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;

public class FlattenedColumnValues implements Externalizable {
    private static final long serialVersionUID = 3691039872299578672L;
    
    private Object _arrayOfValues;
    private boolean[] _nullFlags;
    
    private transient ArrayAccess _arrayAccessor;
    
    /**
     * Default constructor needed for Serialisation.
     *
     */
    public FlattenedColumnValues() {
    }
    
    FlattenedColumnValues(Object arrayOfValues, boolean[] nullFlags){
    	_arrayOfValues = arrayOfValues;
    	_nullFlags = nullFlags;
    	Class componentType = _arrayOfValues.getClass().getComponentType();
        if(componentType.isPrimitive()) {
            _arrayAccessor = ArrayAccessors.getArrayAccessorForPrimitiveType(componentType);
        }
        else {
            _arrayAccessor = ArrayAccessors.getObjectArrayAccessor();
        }
    }
    
    FlattenedColumnValues(Class clazz, int size) {
        // Any of these types ? boolean, byte, char, short, int, long, float, and double
        if(clazz.isPrimitive()) {
            _arrayOfValues = Array.newInstance(clazz, size);
            _nullFlags = new boolean[size];
            _arrayAccessor = ArrayAccessors.getArrayAccessorForPrimitiveType(clazz);
        }
        else {
            _arrayOfValues = Array.newInstance(clazz, size);
            _nullFlags = null;
            _arrayAccessor = ArrayAccessors.getObjectArrayAccessor();
        }
    }
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _arrayOfValues = in.readObject();
        _nullFlags = (boolean[])in.readObject();
        Class componentType = _arrayOfValues.getClass().getComponentType();
        if(componentType.isPrimitive()) {
            _arrayAccessor = ArrayAccessors.getArrayAccessorForPrimitiveType(componentType);
        }
        else {
            _arrayAccessor = ArrayAccessors.getObjectArrayAccessor();
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_arrayOfValues);
        out.writeObject(_nullFlags);
    }
    
    void setObject(int index, Object value) {
        ensureCapacity(index + 1);
        Array.set(_arrayOfValues, index, value);
    }
    
    void setBoolean(int index, boolean value) {
        ensureCapacity(index + 1);
        Array.setBoolean(_arrayOfValues, index, value);
    }

    void setByte(int index, byte value) {
        ensureCapacity(index + 1);
        Array.setByte(_arrayOfValues, index, value);
    }

    void setShort(int index, short value) {
        ensureCapacity(index + 1);
        Array.setShort(_arrayOfValues, index, value);
    }

    void setInt(int index, int value) {
        ensureCapacity(index + 1);
        Array.setInt(_arrayOfValues, index, value);
    }

    void setLong(int index, long value) {
        ensureCapacity(index + 1);
        Array.setLong(_arrayOfValues, index, value);
    }

    void setFloat(int index, float value) {
        ensureCapacity(index + 1);
        Array.setFloat(_arrayOfValues, index, value);
    }

    void setDouble(int index, double value) {
        ensureCapacity(index + 1);
        Array.setDouble(_arrayOfValues, index, value);
    }

    void setIsNull(int index) {
        ensureCapacity(index + 1);
        if(_nullFlags != null) {
            _nullFlags[index] = true;
        }
    }
    
    Object getValue(int index) {
        return _arrayAccessor.getValue(_arrayOfValues, index, _nullFlags);
    }
        
    void ensureCapacity(int minCapacity) {
        // This algorithm is actually copied from the ArrayList implementation. Seems to
        // be a good strategy to grow a statically sized array.
    	int oldCapacity = Array.getLength(_arrayOfValues);
    	if (minCapacity > oldCapacity) {
    	    int newCapacity = (oldCapacity * 3)/2 + 1;
      	    if (newCapacity < minCapacity) {
      	        newCapacity = minCapacity;
      	    }
    	    Object tmpArrayOfValues = _arrayOfValues;
    	    _arrayOfValues = Array.newInstance(tmpArrayOfValues.getClass().getComponentType(), newCapacity);
    	    System.arraycopy(tmpArrayOfValues, 0, _arrayOfValues, 0, Array.getLength(tmpArrayOfValues));
    	    if(_nullFlags != null) {
    	        boolean[] tmpNullFlags = _nullFlags;
    	        _nullFlags = new boolean[newCapacity];
    	        System.arraycopy(tmpNullFlags, 0, _nullFlags, 0, tmpNullFlags.length);
    	    }
    	}
    }

	Object getArrayOfValues() {
		return _arrayOfValues;
	}

	boolean[] getNullFlags() {
		return _nullFlags;
	}
	
	void merge(FlattenedColumnValues fcv){
		int length1 = Array.getLength(_arrayOfValues);
		int length2 = Array.getLength(fcv._arrayOfValues);
		int newCapacity = length1 + length2;
		Object arrayOfValues = Array.newInstance(_arrayOfValues.getClass().getComponentType(), newCapacity);
		System.arraycopy(_arrayOfValues, 0, arrayOfValues, 0, length1);
		System.arraycopy(fcv._arrayOfValues, 0, arrayOfValues, length1, length2);
		_arrayOfValues = arrayOfValues;
		if (_nullFlags!=null || fcv._nullFlags!=null){
			boolean[] nullFlags = new boolean[newCapacity];
			if (_nullFlags!=null){
				System.arraycopy(_nullFlags, 0, nullFlags, 0, length1);
			}
			if (fcv._nullFlags!=null){
				System.arraycopy(fcv._nullFlags, 0, nullFlags, length1, length2);
			}
			_nullFlags = nullFlags;
		}
	}
	
}
