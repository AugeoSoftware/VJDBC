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

public class StatementQueryCommand implements Command, ResultSetProducerCommand,KryoSerializable {
    static final long serialVersionUID = -8463588846424302034L;

    private int _resultSetType;
    private String _sql;

    public StatementQueryCommand() {
    }

    public StatementQueryCommand(String sql, int resultSetType) {
        _sql = sql;
        _resultSetType = resultSetType;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_resultSetType);
        out.writeObject(_sql);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _resultSetType = in.readInt();
        _sql = (String) in.readObject();
    }

    public int getResultSetType() {
        return _resultSetType;
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        return ((Statement) target).executeQuery(ctx.resolveOrCheckQuery(_sql));
    }

    public String toString() {
        return "StatementQueryCommand: " + _sql;
    }

	@Override
	public void write(Kryo kryo, Output output) {
		kryo.writeObjectOrNull(output, _sql, String.class);
		output.writeInt(_resultSetType);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_sql = kryo.readObjectOrNull(input, String.class);
		_resultSetType = input.readInt();		
	}
}
