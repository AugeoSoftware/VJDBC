package de.simplicit.vjdbc.command;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DestroyCommandSerializer extends Serializer<DestroyCommand> {

	@Override
	public void write(Kryo kryo, Output output, DestroyCommand object) {
		output.writeLong(object.getUID().longValue());
		output.writeInt(object.getInterfaceType());
	}

	@Override
	public DestroyCommand read(Kryo kryo, Input input, Class<DestroyCommand> type) {
		return new DestroyCommand(Long.valueOf(input.readLong()), input.readInt());
	}

}
