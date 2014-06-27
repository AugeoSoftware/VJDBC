// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.command;

import de.simplicit.vjdbc.server.command.ResultSetHolder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NextRowPacketCommand implements Command,KryoSerializable {
    static final long serialVersionUID = -8463588846424302034L;

    public NextRowPacketCommand() {
    }

    public void writeExternal(ObjectOutput out) throws IOException {
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    }

    public Object execute(Object target, ConnectionContext ctx) throws SQLException {
        ResultSetHolder rsh = (ResultSetHolder) target;
        // Return next serialized RowPacket
        return rsh.nextRowPacket();
    }

    public String toString() {
        return "NextRowPacketCommand";
    }

	@Override
	public void write(Kryo kryo, Output output) {
	}

	@Override
	public void read(Kryo kryo, Input input) {
	}
}
