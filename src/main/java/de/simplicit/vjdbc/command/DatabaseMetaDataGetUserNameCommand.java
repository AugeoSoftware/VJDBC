package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.VJdbcProperties;

public class DatabaseMetaDataGetUserNameCommand implements Command, KryoSerializable {

	static final long serialVersionUID = 3543492350930057039L;;
	
	
	public DatabaseMetaDataGetUserNameCommand() {
		
	}
	
	public void writeExternal(ObjectOutput out) throws IOException {
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	}

	public Object execute(Object target, ConnectionContext ctx) throws SQLException {
		Object userName = ctx.getClientInfo().get(VJdbcProperties.USER_NAME);
		if (userName==null || "".equals(userName)){
			userName = ((DatabaseMetaData)target).getUserName();
		}
		return userName;
	}

	@Override
	public void write(Kryo kryo, Output output) {
	}

	@Override
	public void read(Kryo kryo, Input input) {
	}

}
