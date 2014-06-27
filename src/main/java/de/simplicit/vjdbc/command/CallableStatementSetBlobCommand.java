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

import de.simplicit.vjdbc.serial.SerialBlob;

public class CallableStatementSetBlobCommand implements Command,KryoSerializable {
    static final long serialVersionUID = 4264932633701227941L;

    private int _index;
    private String _parameterName;
    private SerialBlob blob;

    public CallableStatementSetBlobCommand() {
    }

    public CallableStatementSetBlobCommand(int index, SerialBlob clob) throws IOException {
        _index = index;
        this.blob = clob;
    }

    public CallableStatementSetBlobCommand(String paramName, SerialBlob clob) throws IOException {
        _parameterName = paramName;
        this.blob = clob;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeUTF(_parameterName);
        out.writeObject(blob);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = in.readUTF();
        blob = (SerialBlob)in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        if(_parameterName != null) {
            cstmt.setBlob(_parameterName, blob);
        } else {
            cstmt.setBlob(_index, blob);
        }
        return null;
    }

    public String toString() {
        return "CallableStatementSetBlobCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
		kryo.writeObjectOrNull(output, blob, SerialBlob.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
		blob = kryo.readObjectOrNull(input, SerialBlob.class);		
	}
}
