// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StatementGetGeneratedKeysCommand implements Command, ResultSetProducerCommand,KryoSerializable {
    static final long serialVersionUID = -6529413105195105196L;

    public StatementGetGeneratedKeysCommand() {
    }

    public void writeExternal(ObjectOutput out) throws IOException {
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    public int getResultSetType() {
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        return ((Statement)target).getGeneratedKeys();
    }

    public String toString() {
        return "StatementGetGeneratedKeysCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
	}

	@Override
	public void read(Kryo kryo, Input input) {
	}
}
