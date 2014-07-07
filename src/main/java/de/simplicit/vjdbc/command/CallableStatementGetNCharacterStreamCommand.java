// VJDBC - Virtual JDBC
// Written by Hunter Payne
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.serial.StreamSerializer;

public class CallableStatementGetNCharacterStreamCommand implements Command, KryoSerializable {
    static final long serialVersionUID = -8218845136435435097L;

    private int _index;
    private String _parameterName;

    public CallableStatementGetNCharacterStreamCommand() {
    }

    public CallableStatementGetNCharacterStreamCommand(int index) {
        _index = index;
    }

    public CallableStatementGetNCharacterStreamCommand(String paramName) {
        _parameterName = paramName;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeObject(_parameterName);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = (String)in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        Reader result;
        if(_parameterName != null) {
            result = cstmt.getNCharacterStream(_parameterName);
        } else {
            result = cstmt.getNCharacterStream(_index);
        }
        try {
            // read reader and return as a char[]
            return StreamSerializer.toCharArray(result);
        } catch (IOException ioe) {
            throw new SQLException(ioe);
        }
    }

    public String toString() {
        return "CallableStatementGetNCharacterStreamCommand";
    }
    
	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
	}
}
