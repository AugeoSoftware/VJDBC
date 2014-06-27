// VJDBC - Virtual JDBC
// Written by Hunter Payne
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import de.simplicit.vjdbc.serial.SerialClob;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CallableStatementSetClobCommand implements Command,KryoSerializable {
    static final long serialVersionUID = 4264932633701227941L;

    private int _index;
    private String _parameterName;
    private SerialClob clob;

    public CallableStatementSetClobCommand() {
    }

    public CallableStatementSetClobCommand(int index, SerialClob clob) throws IOException {
        _index = index;
        this.clob = clob;
    }

    public CallableStatementSetClobCommand(String paramName, SerialClob clob) throws IOException {
        _parameterName = paramName;
        this.clob = clob;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeUTF(_parameterName);
        out.writeObject(clob);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = in.readUTF();
        clob = (SerialClob)in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        if(_parameterName != null) {
            cstmt.setClob(_parameterName, clob);
        } else {
            cstmt.setClob(_index, clob);
        }

        return null;
    }

    public String toString() {
        return "CallableStatementSetClobCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
		kryo.writeObjectOrNull(output, clob, SerialClob.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
		clob = kryo.readObjectOrNull(input, SerialClob.class);		
	}
}
