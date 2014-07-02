package de.simplicit.vjdbc.parameters;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ObjectParameterSerializer extends Serializer<ObjectParameter> {

	@Override
	public void write(Kryo kryo, Output output, ObjectParameter object) {
		int argsCount = object.getArgsCount();
		output.writeInt(argsCount);
		kryo.writeClassAndObject(output, object.getValue());
		if (argsCount>1){
			output.writeInt(object.getTargetSqlType());
			if(argsCount>2){
				output.writeInt(object.getScale());
			}
		}
	}

	@Override
	public ObjectParameter read(Kryo kryo, Input input, Class<ObjectParameter> type) {
		int argsCount = input.readInt();
		switch (argsCount){
		case 1:
			return new ObjectParameter(kryo.readClassAndObject(input));
		case 2:
			return new ObjectParameter(kryo.readClassAndObject(input), input.readInt());
		case 3: 
			return new ObjectParameter(kryo.readClassAndObject(input), input.readInt(), input.readInt());
		}
		return null;
	}

}
