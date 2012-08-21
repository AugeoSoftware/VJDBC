// VJDBC - Virtual JDBC
// Written by Hunter Payne
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.RowId;
import java.sql.SQLException;

public class SerialRowId implements RowId, Externalizable {
    static final long serialVersionUID = 3359567957805294836L;

    private byte[] bytes;
    private String str;
    private int hashCode;

    public SerialRowId(){
    }
    
   
    public SerialRowId(RowId rowId, String str) throws SQLException {
        bytes = rowId.getBytes();
        this.str = str;
        hashCode = rowId.hashCode();
    }
    
    public SerialRowId(RowId rowId) throws SQLException {
    	this(rowId, rowId.toString());
//        bytes = rowId.getBytes();
//        str = rowId.toString();
//        hashCode = rowId.hashCode();
    }

    public boolean equals(Object o) {
        if (o instanceof RowId) {
            RowId rowId = (RowId)o;
            byte[] otherBytes = rowId.getBytes();
            if (bytes.length == otherBytes.length) {
                for (int i = 0; i < bytes.length; ++i) {
                    if (bytes[i] != otherBytes[i]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public String toString() {
        return str;
    }

    public int hashCode() {
        return hashCode;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
        out.writeUTF(str);
        out.writeInt(hashCode);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();
        bytes = new byte[len];
        in.read(bytes);
        str = in.readUTF();
        hashCode = in.readInt();
    }
}
