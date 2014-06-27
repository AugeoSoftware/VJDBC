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

import de.simplicit.vjdbc.serial.SerialRowId;

public class CallableStatementSetRowIdCommand implements Command,KryoSerializable {
    static final long serialVersionUID = -2847792562974087927L;

    private int _index;
    private String _parameterName;
    private SerialRowId rowId;

    public CallableStatementSetRowIdCommand() {
    }

    public CallableStatementSetRowIdCommand(int index, SerialRowId rowId) throws IOException {
        _index = index;
        this.rowId = rowId;
    }

    public CallableStatementSetRowIdCommand(String paramName, SerialRowId rowId) throws IOException {
        _parameterName = paramName;
        this.rowId = rowId;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeUTF(_parameterName);
        out.writeObject(rowId);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = in.readUTF();
        rowId = (SerialRowId)in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        if(_parameterName != null) {
            cstmt.setRowId(_parameterName, rowId);
        } else {
            cstmt.setRowId(_index, rowId);
        }

        return null;
    }

    public String toString() {
        return "CallableStatementSetRowIdCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
		kryo.writeObjectOrNull(output, rowId, SerialRowId.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
		rowId = kryo.readObjectOrNull(input, SerialRowId.class);
	}
}
