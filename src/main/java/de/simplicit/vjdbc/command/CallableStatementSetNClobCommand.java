// VJDBC - Virtual JDBC
// Written by Hunter Payne
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.serial.SerialNClob;

public class CallableStatementSetNClobCommand implements Command,KryoSerializable {
    static final long serialVersionUID = 4264932633701227941L;

    private int _index;
    private String _parameterName;
    private SerialNClob nclob;

    public CallableStatementSetNClobCommand() {
    }

    public CallableStatementSetNClobCommand(int index, SerialNClob nclob) throws IOException {
        _index = index;
        this.nclob = nclob;
    }

    public CallableStatementSetNClobCommand(String paramName, SerialNClob nclob) throws IOException {
        _parameterName = paramName;
        this.nclob = nclob;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeUTF(_parameterName);
        out.writeObject(nclob);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = in.readUTF();
        nclob = (SerialNClob)in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        if(_parameterName != null) {
            cstmt.setNClob(_parameterName, nclob);
        } else {
            cstmt.setNClob(_index, nclob);
        }

        return null;
    }

    public String toString() {
        return "CallableStatementSetNClobCommand";
    }
	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
		kryo.writeObjectOrNull(output, nclob, SerialNClob.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
		nclob = kryo.readObjectOrNull(input, SerialNClob.class);		
	}
}
