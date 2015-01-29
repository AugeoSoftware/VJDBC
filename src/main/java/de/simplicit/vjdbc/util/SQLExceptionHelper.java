// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.util;

import java.sql.SQLException;
import java.util.Iterator;

/**
 * SQLExceptionHelper wraps driver-specific exceptions in a generic SQLException.
 */
public class SQLExceptionHelper {

	public static final String CONNECTION_DOES_NOT_EXIST_STATE = "08003";

    public static SQLException wrap(Throwable t) {
 
    	if (t==null || t instanceof SQLException){
    		return wrap((SQLException)t);
    	}
    	if (isExceptionSerializable(t)){
    		return new SQLException(t.getMessage(), t);
    	}
    	return wrapThrowable(t);
    }
    
    
    public static SQLException wrap(SQLException ex) {

    	if (ex==null || 
    			// some driver may extend SQLException, but the client JRE may not have this class
    			// filter out such classes
    			(ex.getClass().getName().startsWith("java.sql.") &&
    					isSQLExceptionSerializable(ex))) {
            // yes a bit misleading but since this exception is already OK
            // for transport, its much simpler just to return it
            return ex;
        }
        else {
            return wrapSQLException(ex);
        }
    }

    private static boolean isExceptionSerializable(Throwable ex) {

        boolean exceptionIsSerializable = true;
        Throwable loop = ex;

        while (loop != null && exceptionIsSerializable) {

            exceptionIsSerializable =
                java.io.Serializable.class.isAssignableFrom(loop.getClass()) ||
                java.io.Externalizable.class.isAssignableFrom(loop.getClass());
            loop = loop.getCause();
        }

        return exceptionIsSerializable;
    }

    private static boolean isSQLExceptionSerializable(SQLException ex) {

        boolean exceptionIsSerializable = true;
        Iterator<Throwable> it = ex.iterator();

        while (it.hasNext() && exceptionIsSerializable) {

            Throwable t = it.next();
            exceptionIsSerializable =
                java.io.Serializable.class.isAssignableFrom(t.getClass()) ||
                java.io.Externalizable.class.isAssignableFrom(t.getClass());
        }

        return exceptionIsSerializable;
    }

    private static SQLException wrapSQLException(SQLException ex) {

        SQLException ex2 =
            new SQLException(ex.getMessage(), ex.getSQLState(),
                             ex.getErrorCode(), wrap(ex.getCause()));

        if (ex.getNextException() != null) {
            ex2.setNextException(wrap(ex.getNextException()));
        }
        ex2.setStackTrace(ex.getStackTrace()); // preserve stack trace
        return ex2;
    }

    private static SQLException wrapThrowable(Throwable t) {

        SQLException wrapped = null;
        if (t instanceof SQLException) {
            wrapped = wrapSQLException((SQLException)t);
        } else {
            wrapped = new SQLException(t.getClass().getName()+": "+t.getMessage(), wrap(t.getCause()));
        }
        // REVIEW: doing some evil hackeration here, but only because I believe
        // that those that change stack traces deserve a special place in hell
        // If your code can be hacked by stack trace info, it deserves to
        // be hacked and will be cracked anyway
        wrapped.setStackTrace(t.getStackTrace());

        return wrapped;
    }
}
