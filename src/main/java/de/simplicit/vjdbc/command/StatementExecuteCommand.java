// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.sql.Statement;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StatementExecuteCommand implements Command,KryoSerializable {
    private static final long serialVersionUID = 3760844562717291058L;

    private String _sql;

    public StatementExecuteCommand() {
    }

    public StatementExecuteCommand(String sql) {
        _sql = sql;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_sql);
    }

    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {
        _sql = (String) in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        return Boolean.valueOf(((Statement) target).execute(ctx.resolveOrCheckQuery(_sql)));
    }

    public String toString() {
        return "StatementExecuteCommand: " + _sql;
    }

	@Override
	public void write(Kryo kryo, Output output) {
		kryo.writeObjectOrNull(output, _sql, String.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_sql = kryo.readObjectOrNull(input, String.class);
	}
}
