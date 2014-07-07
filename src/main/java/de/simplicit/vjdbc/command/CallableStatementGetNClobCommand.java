// VJDBC - Virtual JDBC
// Written by Hunter Payne
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.CallableStatement;
import java.sql.NClob;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.serial.SerialNClob;

public class CallableStatementGetNClobCommand implements Command, KryoSerializable {
    static final long serialVersionUID = 8230491873823084213L;

    private int _index;
    private String _parameterName;

    public CallableStatementGetNClobCommand() {
    }

    public CallableStatementGetNClobCommand(int index) {
        _index = index;
    }

    public CallableStatementGetNClobCommand(String paramName) {
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
        NClob result;
        if(_parameterName != null) {
            result = cstmt.getNClob(_parameterName);
        } else {
            result = cstmt.getNClob(_index);
        }
        return new SerialNClob(result);
    }

    public String toString() {
        return "CallableStatementGetNClobCommand";
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
