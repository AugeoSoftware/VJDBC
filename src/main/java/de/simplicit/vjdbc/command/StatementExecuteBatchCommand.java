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

public class StatementExecuteBatchCommand implements Command,KryoSerializable {
    static final long serialVersionUID = -995205757280796006L;

    private String[] _sql;

    public StatementExecuteBatchCommand() {
    }

    public StatementExecuteBatchCommand(String[] sql) {
        _sql = sql;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_sql);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _sql = (String[])in.readObject();
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        Statement stmt = (Statement)target;
        stmt.clearBatch();
        for(int i = 0; i < _sql.length; i++) {
            stmt.addBatch(ctx.resolveOrCheckQuery(_sql[i]));
        }
        return stmt.executeBatch();
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < _sql.length; i++) {
            sb.append(_sql[i]);
            sb.append('\n');
        }
        return "StatementExecuteBatchCommand:\n" + sb.toString();
    }

	@Override
	public void write(Kryo kryo, Output output) {
		kryo.writeObjectOrNull(output, _sql, String[].class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_sql = kryo.readObjectOrNull(input, String[].class);
	}
}
