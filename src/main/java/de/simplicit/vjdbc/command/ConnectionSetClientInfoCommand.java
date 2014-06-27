package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ConnectionSetClientInfoCommand implements Command,Serializable, KryoSerializable {

	private String _name;
	private String _value;
	
	public ConnectionSetClientInfoCommand(){		
	}
	
	public ConnectionSetClientInfoCommand(String name, String value){
		_name = name;
		_value = value;
	}
	
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(_name);
		out.writeObject(_value);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		_name = (String) in.readObject();
		_value = (String) in.readObject();
	}

	@Override
	public Object execute(Object target, ConnectionContext ctx) throws SQLException {		
		ctx.setClientInfo(_name, _value);
		return null;
	}

	public String getName() {
		return _name;
	}

	public String getValue() {
		return _value;
	}

	@Override
	public void write(Kryo kryo, Output output) {
		kryo.writeObjectOrNull(output, _name, String.class);
		kryo.writeObjectOrNull(output, _value, String.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_name = kryo.readObjectOrNull(input, String.class);
		_value = kryo.readObjectOrNull(input, String.class);
	}
}
