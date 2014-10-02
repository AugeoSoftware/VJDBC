// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicLong;

public class UIDEx implements Externalizable {
    static final long serialVersionUID = 1682984916549281270L;

    private static AtomicLong s_cookie = new AtomicLong(1L);
    /**
     * negative values are reserved for usage in CompositeCommand
     */
    private Long _uid = new Long(s_cookie.getAndIncrement() & 0x7FFFFFFFFFFFFFFFL);
    private int _value1 = Integer.MIN_VALUE;
    private int _value2 = Integer.MIN_VALUE;

    public UIDEx() {
    }

    public UIDEx(int value1) {
        _value1 = value1;
    }

    public UIDEx(int value1, int value2) {
        _value1 = value1;
        _value2 = value2;
    }

//    public UIDEx(Long uid, int value1) {
//        _uid = uid;
//        _value1 = value1;
//    }

    public UIDEx(Long uid, int value1, int value2) {
    	_uid = uid;
        _value1 = value1;
        _value2 = value2;
    }

    public Long getUID() {
        return _uid;
    }

    public int getValue1() {
        return _value1;
    }

    public int getValue2() {
        return _value2;
    }

    public void resetValues() {
        _value1 = Integer.MIN_VALUE;
        _value2 = Integer.MIN_VALUE;
    }

    public int hashCode() {
        return _uid.hashCode();
    }

    public boolean equals(Object obj) {
        return (obj instanceof UIDEx) && (_uid.equals(((UIDEx)obj)._uid));
    }
    
    public String toString() {
        return _uid.toString();
    }
    /**
     * copy all info from given instance
     * @param reg
     */
    public void copyFrom(UIDEx reg){
    	assert this._uid<0 : "Changing valid UIDEx is prohibited"; 
    	this._uid = reg._uid;
    	this._value1 = reg._value1;
    	this._value2 = reg._value2;
    }
    
    
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_uid.longValue());
        out.writeInt(_value1);
        out.writeInt(_value2);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _uid = new Long(in.readLong());
        _value1 = in.readInt();
        _value2 = in.readInt();
    }
}
