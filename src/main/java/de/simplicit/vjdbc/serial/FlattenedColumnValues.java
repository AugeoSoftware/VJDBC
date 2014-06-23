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
    private int[] _nullFlags = null;
    
    private transient Class componentType;
    
    /**
     * Default constructor needed for Serialisation.
     *
     */
    public FlattenedColumnValues() {
    }
    
    FlattenedColumnValues(Object arrayOfValues, int[] nullFlags){
    	_arrayOfValues = arrayOfValues;
    	_nullFlags = nullFlags;
    	componentType = _arrayOfValues.getClass().getComponentType();
    }
    
    FlattenedColumnValues(Class clazz, int size) {
        // Any of these types ? boolean, byte, char, short, int, long, float, and double
    	_arrayOfValues = Array.newInstance(clazz, size);
    	componentType = clazz;
    	if(clazz.isPrimitive()) {
    		_nullFlags = new int[(size>>5) + 1];
    	}
    }
    
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _arrayOfValues = in.readObject();
        _nullFlags = (int[])in.readObject();
        componentType = _arrayOfValues.getClass().getComponentType();
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
            int i = index>>5;
            int m = 1<<(index & 31);
            _nullFlags[i] = _nullFlags[i] | m;
        }
    }
    
    Object getValue(int index) {
        int i = index>>5;
        int m = 1<<(index & 31);
        if (_nullFlags!=null && (_nullFlags[i] & m)!=0){
        	return null;
        }
        if (!componentType.isPrimitive()){
        	return Array.get(_arrayOfValues, index);
        } else if (componentType == Boolean.TYPE) {
			boolean[] v = (boolean[]) _arrayOfValues;
			return Boolean.valueOf(v[index]);
		} else if (componentType == Byte.TYPE) {
			byte[] v = (byte[]) _arrayOfValues;
			return Byte.valueOf(v[index]);
		}  else if (componentType == Short.TYPE) {
			short[] v = (short[]) _arrayOfValues;
			return Short.valueOf(v[index]);
		} else if (componentType == Integer.TYPE) {
			int[] v = (int[]) _arrayOfValues;
			return Integer.valueOf(v[index]);
		} else if (componentType == Long.TYPE) {
			long[] v = (long[]) _arrayOfValues;
			return Long.valueOf(v[index]);
		} else if (componentType == Float.TYPE) {
			float[] v = (float[]) _arrayOfValues;
			return new Float(v[index]);					
		} else if (componentType == Double.TYPE) {
			double[] v = (double[]) _arrayOfValues;
			return new Double(v[index]);
		} else if (componentType == Character.TYPE) {
			char[] v = (char[]) _arrayOfValues;
			return Character.valueOf(v[i]);
		}
		return null;
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
    	        int[] tmpNullFlags = _nullFlags;
    	        _nullFlags = new int[(newCapacity>>5) + 1];
    	        System.arraycopy(tmpNullFlags, 0, _nullFlags, 0, tmpNullFlags.length);
    	    }
    	}
    }

	Object getArrayOfValues() {
		return _arrayOfValues;
	}

	int[] getNullFlags() {
		return _nullFlags;
	}

}
