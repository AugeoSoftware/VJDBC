// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ConnectionReleaseSavepointCommand implements Command, KryoSerializable {
    static final long serialVersionUID = 6221665321426908025L;

    private Long _uidOfSavepoint;

    public ConnectionReleaseSavepointCommand() {
    }

    public ConnectionReleaseSavepointCommand(Long uidOfSavepoint) {
        _uidOfSavepoint = uidOfSavepoint;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_uidOfSavepoint.longValue());
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _uidOfSavepoint = new Long(in.readLong());
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        Savepoint sp = (Savepoint)ctx.getJDBCObject(_uidOfSavepoint);
        ((Connection)target).releaseSavepoint(sp);
        return null;
    }

    public String toString() {
        return "ConnectionReleaseSavepointCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeLong(_uidOfSavepoint.longValue());
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_uidOfSavepoint = Long.valueOf(input.readLong());
	}
    
    
}
