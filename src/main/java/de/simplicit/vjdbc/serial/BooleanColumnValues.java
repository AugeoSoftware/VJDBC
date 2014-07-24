package de.simplicit.vjdbc.serial;

public class BooleanColumnValues extends ColumnValues {

	private int[] values = null;
	private int[] nullFlags = null;
	
	BooleanColumnValues(int[] values, int[] nullFlags, int size) {
		super(Boolean.TYPE);
		this.values = values;
		this.nullFlags = nullFlags;
		this.size = size;
	}

	BooleanColumnValues( int initialSize) {
		super(Boolean.TYPE);

		int size = (initialSize >> 5) + 1;
		values = new int[size];
		nullFlags = new int[size];
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
			values = new int[(newCapacity >> 5) + 1];
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
	final void setBoolean(int index, boolean value) {
		int i = index >> 5;
		int m = 1 << (index & 31);
		if (value){
			values[i] = values[i] | m;
		} else {
			values[i] = values[i] & ~m;
		}
	}

	@Override
	final boolean getBoolean(int index) {
		int i = index >> 5;
		int m = 1 << (index & 31);
		return (values[i] & m)!=0;
	}

	@Override
	final Object getObject(int index) {
		return Boolean.valueOf(getBoolean(index));
	}

	@Override
	final Object getValues() {
		return values;
	}

	final int[] getNullFlags() {
		return nullFlags;
	}
	
}
