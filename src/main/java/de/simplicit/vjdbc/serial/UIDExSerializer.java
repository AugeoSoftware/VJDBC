package de.simplicit.vjdbc.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class UIDExSerializer extends Serializer<UIDEx> {

	@Override
	public void write(Kryo kryo, Output output, UIDEx object) {
		output.writeLong(object.getUID());
		output.writeInt(object.getValue1());
		output.writeInt(object.getValue2());
	}

	@Override
	public UIDEx read(Kryo kryo, Input input, Class<UIDEx> type) {
		return new UIDEx(input.readLong(), input.readInt(), input.readInt());
	}

}
