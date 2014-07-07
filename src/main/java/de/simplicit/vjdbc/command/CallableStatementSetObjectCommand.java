// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CallableStatementSetObjectCommand implements Command,KryoSerializable {
    static final long serialVersionUID = -9132697894345849726L;

//    private int _index;
    private String _paramName;
    private int _argsCount;
    private int _targetSqlType;
    private int _scale;
    private Object _object;

    // default constructor for serialization
    public CallableStatementSetObjectCommand() {
    }

    public CallableStatementSetObjectCommand(String paramName) {
    	_paramName = paramName;
    	_argsCount = 1;
    }
    
//    public CallableStatementSetObjectCommand(int index, Integer targetSqlType, Integer scale) {
//        _index = index;
//        _targetSqlType = targetSqlType;
//        _scale = scale;
//        _transport = null;
//    }

    public CallableStatementSetObjectCommand(String paramName, int targetSqlType) {
        _paramName = paramName;
        _targetSqlType = targetSqlType;
        _object = null;
        _argsCount = 2;
    }
    
    public CallableStatementSetObjectCommand(String paramName, int targetSqlType, int scale) {
        _paramName = paramName;
        _targetSqlType = targetSqlType;
        _scale = scale;
        _object = null;
        _argsCount = 3;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
//        out.writeInt(_index);
    	out.writeInt(_argsCount);
        out.writeObject(_paramName);
        out.writeObject(_object);
        if (_argsCount>1){
        	out.writeInt(_targetSqlType);
        	if (_argsCount>2){
        		out.writeInt(_scale);
        	}
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _argsCount = in.readInt();
        _paramName = (String)in.readObject();
        _object = in.readObject();
        if (_argsCount>1){
        	_targetSqlType = in.readInt();
        	if (_argsCount>2){
        		_scale = in.readInt();
        	}
        }
    }

    public void setObject(Object obj) throws SQLException {
        if(obj instanceof Serializable) {
            _object = obj;
        } else {
            throw new SQLException("Object of type " + obj.getClass().getName() + " is not serializable");
        }
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        CallableStatement cstmt = (CallableStatement)target;

        switch(_argsCount){
        case 1:
        	cstmt.setObject(_paramName, _object);
        	break;
        case 2:
        	cstmt.setObject(_paramName, _object, _targetSqlType);
        	break;
        case 3:
        	cstmt.setObject(_paramName, _object, _targetSqlType, _scale);
        }
        
//        if(_paramName != null) {
//            if(_targetSqlType != null) {
//                if(_scale != null) {
//                    cstmt.setObject(_paramName, obj, _targetSqlType.intValue(), _scale.intValue());
//                } else {
//                    cstmt.setObject(_paramName, obj, _targetSqlType.intValue());
//                }
//            } else {
//                cstmt.setObject(_paramName, obj);
//            }
//        } else {
//            if(_targetSqlType != null) {
//                if(_scale != null) {
//                    cstmt.setObject(_index, obj, _targetSqlType.intValue(), _scale.intValue());
//                } else {
//                    cstmt.setObject(_index, obj, _targetSqlType.intValue());
//                }
//            } else {
//                cstmt.setObject(_index, obj);
//            }
//        }

        return null;
    }

    public String toString() {
        return "CallableStatementSetObjectCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_argsCount);
		kryo.writeObjectOrNull(output, _paramName, String.class);
		kryo.writeClassAndObject(output, _object);
        if (_argsCount>1){
        	output.writeInt(_targetSqlType);
        	if (_argsCount>2){
        		output.writeInt(_scale);
        	}
        }
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_argsCount = input.readInt();
		_paramName = kryo.readObjectOrNull(input, String.class);
		_object = kryo.readClassAndObject(input);
        if (_argsCount>1){
        	_targetSqlType = input.readInt();        	
        	if (_argsCount>2){
        		_scale = input.readInt();
        	}
        }
	}
}
