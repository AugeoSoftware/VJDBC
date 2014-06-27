// VJDBC - Virtual JDBC
// Written by Hunter Payne
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import de.simplicit.vjdbc.serial.StreamSerializer;

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

public class CallableStatementSetNCharacterStreamCommand implements Command,KryoSerializable {
    static final long serialVersionUID = 8952810867158345906L;

    private int _index;
    private int _length;
    private String _parameterName;
    private char[] _charArray;

    public CallableStatementSetNCharacterStreamCommand() {
    }

    public CallableStatementSetNCharacterStreamCommand(int index, Reader reader) throws IOException {
        _index = index;
        _charArray = StreamSerializer.toCharArray(reader);
        _length = _charArray.length;
    }

    public CallableStatementSetNCharacterStreamCommand(String paramName, Reader reader) throws IOException {
        _parameterName = paramName;
        _charArray = StreamSerializer.toCharArray(reader);
        _length = _charArray.length;
    }

    public CallableStatementSetNCharacterStreamCommand(int index, Reader reader, int len) throws IOException {
        _index = index;
        _length = len;
        _charArray = StreamSerializer.toCharArray(reader, len);
    }

    public CallableStatementSetNCharacterStreamCommand(String paramName, Reader reader, int len) throws IOException {
        _parameterName = paramName;
        _length = len;
        _charArray = StreamSerializer.toCharArray(reader, len);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeInt(_length);
        out.writeObject(_parameterName);
        out.writeObject(_charArray);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _length = in.readInt();
        _parameterName = (String)in.readObject();
        _charArray = (char[])in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        Reader reader = StreamSerializer.toReader(_charArray);
        if(_parameterName != null) {
            cstmt.setNCharacterStream(_parameterName, reader, _length);
        } else {
            cstmt.setNCharacterStream(_index, reader, _length);
        }

        return null;
    }

    public String toString() {
        return "CallableStatementSetNCharacterStreamCommand";
    }
    
	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		output.writeInt(_length);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
		kryo.writeObjectOrNull(output, _charArray, char[].class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_length = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
		_charArray = kryo.readObjectOrNull(input, char[].class);
	}
}
