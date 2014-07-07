// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.VirtualCallableStatement;

public class CallableStatementGetObjectCommand implements Command, KryoSerializable {
    static final long serialVersionUID = 7045834396073252820L;

    private int _index;
    private String _parameterName;
    private Map _map;
    private Class _clazz;

    public CallableStatementGetObjectCommand() {
    }

    public CallableStatementGetObjectCommand(int index) {
        _index = index;
        _map = null;
        _clazz = null;
    }

    public CallableStatementGetObjectCommand(int index, Class clazz) {
        _index = index;
        _map = null;
        _clazz = clazz;
    }

    public CallableStatementGetObjectCommand(int index, Map map) {
        _index = index;
        _map = map;
        _clazz = null;
    }

    public CallableStatementGetObjectCommand(String paramName) {
        _parameterName = paramName;
        _map = null;
        _clazz = null;
    }

    public CallableStatementGetObjectCommand(String paramName, Class clazz) {
        _parameterName = paramName;
        _map = null;
        _clazz = clazz;
    }

    public CallableStatementGetObjectCommand(String paramName, Map map) {
        _parameterName = paramName;
        _map = map;
        _clazz = null;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_index);
        out.writeObject(_parameterName);
        out.writeObject(_map);

        out.writeBoolean(_clazz != null);
        if (_clazz != null) out.writeUTF(_clazz.getName());
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _index = in.readInt();
        _parameterName = (String)in.readObject();
        _map = (Map)in.readObject();
        if (in.readBoolean())
            _clazz = Class.forName(in.readUTF());
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;
        Object result;

        if(_parameterName != null) {
            if(_map != null) {
                result = cstmt.getObject(_parameterName, _map);
            } else if (_clazz != null) {
                result =
                    ((VirtualCallableStatement)cstmt).getObject(_parameterName,
                                                                _clazz);
            } else {
                result = cstmt.getObject(_parameterName);
            }
        } else {
            if(_map != null) {
                result = cstmt.getObject(_index, _map);
            } else if (_clazz != null) {
                result =
                    ((VirtualCallableStatement)cstmt).getObject(_index,
                                                                _clazz);
            } else {
                result = cstmt.getObject(_index);
            }
        }

        // ResultSets are handled by the caller
        if(result instanceof ResultSet) {
            return result;
        }

        // Any other type must be Serializable to be transported
        if(result == null || result instanceof Serializable) {
            return result;
        }

        throw new SQLException("Object of type " + result.getClass().getName() + " is not serializable");
    }

    public String toString() {
        return "CallableStatementGetObjectCommand";
    }
    
	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_index);
		kryo.writeObjectOrNull(output, _parameterName, String.class);
		kryo.writeObjectOrNull(output, _clazz, Class.class);
		kryo.writeClassAndObject(output, _map);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_index = input.readInt();
		_parameterName = kryo.readObjectOrNull(input, String.class);
		_clazz = kryo.readObjectOrNull(input, Class.class);
		_map = (Map) kryo.readClassAndObject(input);
	}
}
