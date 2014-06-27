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

import de.simplicit.vjdbc.serial.SerialSQLXML;

public class CallableStatementSetSQLXMLCommand implements Command,KryoSerializable {
    static final long serialVersionUID = 7396654168665073844L;

    private int _index;
    private String _parameterName;
    private SerialSQLXML sqlxml;

    public CallableStatementSetSQLXMLCommand() {
    }

    public CallableStatementSetSQLXMLCommand(int index, SerialSQLXML sqlxml) throws IOException {
        _index = index;
        this.sqlxml = sqlxml;
    }

    public CallableStatementSetSQLXMLCommand(String paramName, SerialSQLXML sqlxml) throws IOException {
        _parameterName = paramName;
        this.sqlxml = sqlxml;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeUTF(_parameterName);
        out.writeObject(sqlxml);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = in.readUTF();
        sqlxml = (SerialSQLXML)in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        if(_parameterName != null) {
            cstmt.setSQLXML(_parameterName, sqlxml);
        } else {
            cstmt.setSQLXML(_index, sqlxml);
        }

        return null;
    }

    public String toString() {
        return "CallableStatementSetSQLXMLCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
		kryo.writeObjectOrNull(output, sqlxml, SerialSQLXML.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
		sqlxml = kryo.readObjectOrNull(input, SerialSQLXML.class);		
	}
}
