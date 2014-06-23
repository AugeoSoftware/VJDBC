package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RowPacketSerializer extends Serializer<RowPacket> {

	@Override
	public void write(Kryo kryo, Output output, RowPacket object) {
		output.writeBoolean(object.isForwardOnly());
		output.writeBoolean(object.isLastPart());
		int rowCount = object.getRowCount();
		output.writeInt(rowCount);
		FlattenedColumnValues[] flattenedColumnsValues = object.getFlattenedColumnsValues();
		if (rowCount > 0 && flattenedColumnsValues!=null) {
			int length = flattenedColumnsValues.length;
			output.writeInt(length);
			for (int i = 0; i < length; i++) {
				Object columnValues = flattenedColumnsValues[i].getArrayOfValues();				
				Class<?> componentType = columnValues.getClass().getComponentType();				
				kryo.writeClass(output, componentType);
				if (componentType == Boolean.TYPE) {
					boolean[] v = (boolean[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeBoolean(v[k]);
					}
				} else if (componentType == Byte.TYPE) {
					byte[] v = (byte[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeByte(v[k]);
					}
				} else if (componentType == Short.TYPE) {
					short[] v = (short[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeShort(v[k]);
					}
				} else if (componentType == Integer.TYPE) {
					int[] v = (int[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeInt(v[k]);
					}
				} else if (componentType == Long.TYPE) {
					long[] v = (long[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeLong(v[k]);
					}
				} else if (componentType == Float.TYPE) {
					float[] v = (float[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeFloat(v[k]);
					}
				} else if (componentType == Double.TYPE) {
					double[] v = (double[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeDouble(v[k]);
					}
				} else if (componentType == Character.TYPE) {
					char[] v = (char[]) columnValues;
					for (int k = 0; k < rowCount; k++) {
						output.writeChar(v[k]);
					}
				} else {
					Object[] v = (Object[]) columnValues;

					if (Object.class.equals(componentType)) {
						for (int k = 0; k < rowCount; k++) {
							kryo.writeClassAndObject(output, v[k]);
						}
					} else {
						for (int k = 0; k < rowCount; k++) {
							kryo.writeObjectOrNull(output, v[k], componentType);
						}
					}
				}
				// write nullFlags
				kryo.writeObjectOrNull(output, flattenedColumnsValues[i].getNullFlags(), int[].class);
			}
		}
	}

	@Override
	public RowPacket read(Kryo kryo, Input input, Class<RowPacket> type) {
		boolean forwardOnly = input.readBoolean();
		boolean lastPart = input.readBoolean();
		int rowCount = input.readInt();		
		FlattenedColumnValues[] flattenedColumnsValues = null;
		if (rowCount>0) {
			int length = input.readInt();
			flattenedColumnsValues = new FlattenedColumnValues[length];
			for (int i=0; i<length; i++){
				Registration registration = kryo.readClass(input);
				if (registration!=null){
					Object columnValues = null;
					Class<?> componentType = registration.getType();
					if (Boolean.TYPE.equals(componentType)){
						boolean[] v = new boolean[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readBoolean();
						}
						columnValues = v;
					} else if (Byte.TYPE.equals(componentType)){
						byte[] v = new byte[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readByte();
						}
						columnValues = v;						
					} else if (Short.TYPE.equals(componentType)){
						short[] v = new short[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readShort();
						}
						columnValues = v;						
					} else if (Integer.TYPE.equals(componentType)){
						int[] v = new int[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readInt();
						}
						columnValues = v;						
					} else if (Long.TYPE.equals(componentType)){
						long[] v = new long[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readLong();
						}
						columnValues = v;
					} else if (Float.TYPE.equals(componentType)){
						float[] v = new float[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readFloat();
						}
						columnValues = v;						
					} else if (Double.TYPE.equals(componentType)){
						double[] v = new double[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readDouble();
						}
						columnValues = v;
					} else if (Character.TYPE.equals(componentType)) {
						char[] v = new char[rowCount];
						for (int k=0; k<rowCount; k++){
							v[k] = input.readChar();
						}
						columnValues = v;
					} else {
						Object[] v = new Object[rowCount];
						if (Object.class.equals(componentType)){
							for (int k=0; k<rowCount; k++){
								v[k] = kryo.readClassAndObject(input);
							}							
						} else {
							for (int k=0; k<rowCount; k++){
								v[k] = kryo.readObjectOrNull(input, componentType);
							}
						}
						columnValues = v;						
					}
					
					int[] nullFlags = kryo.readObjectOrNull(input, int[].class); // read nullFlags
					flattenedColumnsValues[i] = new FlattenedColumnValues(columnValues, nullFlags);
				} else {
					// TODO log error
				}
			}
		}
		return new RowPacket(forwardOnly, lastPart, rowCount, flattenedColumnsValues);
	}

}
