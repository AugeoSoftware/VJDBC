// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.parameters;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ObjectParameter implements PreparedStatementParameter {
    static final long serialVersionUID = -9065375715201787003L;

    private int _argsCount;
    private Object _value;
    private int _targetSqlType;
    private int _scale;
    
    public ObjectParameter() {
    }

    public ObjectParameter(Object value) {
        _value = value;
        _argsCount = 1;
    }
    
    public ObjectParameter(Object value, int targetSqlType) {
        _value = value;
        _targetSqlType = targetSqlType;
        _argsCount = 2;
    }
    
    public ObjectParameter(Object value, int targetSqlType, int scale) {
        _value = value;
        _targetSqlType = targetSqlType;
        _scale = scale;
        _argsCount = 3;
    }
    
    public Object getValue() {
        return _value;
    }

    public int getArgsCount() {
		return _argsCount;
	}

	public int getTargetSqlType() {
		return _targetSqlType;
	}

	public int getScale() {
		return _scale;
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _argsCount = in.readInt();
    	_value = in.readObject();
        _targetSqlType = (Integer)in.readObject();
        _scale = (Integer)in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_argsCount);
    	out.writeObject(_value);
        out.writeObject(_targetSqlType);
        out.writeObject(_scale);
    }

    public void setParameter(PreparedStatement pstmt, int index) throws SQLException {
        switch (_argsCount) {
		case 1:
			pstmt.setObject(index, _value);
			break;
		case 2:
			pstmt.setObject(index, _value, _targetSqlType);
			break;
		case 3:
			pstmt.setObject(index, _value, _targetSqlType, _scale);
			break;
		}
    }

    public String toString() {
        return "Object: " + _value;
    }
}
