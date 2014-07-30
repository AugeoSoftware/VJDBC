// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.server.command;

import de.simplicit.vjdbc.serial.CallingContext;

class JdbcObjectHolder {
    private final Object _jdbcObject;
    private final CallingContext _callingContext;
    private final int _jdbcInterfaceType;
    
    JdbcObjectHolder(Object jdbcObject, CallingContext ctx, int _jdbcInterfaceType) {
        this._jdbcObject = jdbcObject;
        this._callingContext = ctx;
        this._jdbcInterfaceType = _jdbcInterfaceType;
    }
    
    final CallingContext getCallingContext() {
        return this._callingContext;
    }

    final Object getJdbcObject() {
        return this._jdbcObject;
    }
    
    final int getJdbcInterfaceType() {
    	return this._jdbcInterfaceType;
    }
}
