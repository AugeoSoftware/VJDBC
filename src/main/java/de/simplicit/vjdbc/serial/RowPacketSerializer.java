package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class RowPacketSerializer extends Serializer<RowPacket> {

	@Override
	public void write(Kryo kryo, Output output, RowPacket object) {
		output.writeInt(object.getIndex());
		output.writeBoolean(object.isLastPart());
		int rowCount = object.getRowCount();
		output.writeInt(rowCount);
		kryo.writeObjectOrNull(output, object.getFlattenedColumnsValues(), ColumnValues[].class);
	}
	
	@Override
	public RowPacket read(Kryo kryo, Input input, Class<RowPacket> type) {
		int index = input.readInt();
		boolean lastPart = input.readBoolean();
		int rowCount = input.readInt();
		ColumnValues[] columnValues = kryo.readObjectOrNull(input,  ColumnValues[].class);
		return new RowPacket(index, lastPart, rowCount, columnValues);
		
	}
}
