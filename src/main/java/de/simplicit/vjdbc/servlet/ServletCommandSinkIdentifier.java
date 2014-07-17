// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.servlet;

/**
 * Common identifiers which are used in the Http-Header to route the requests
 * to the corresponding handler.
 * @author Mike
 *
 */
public interface ServletCommandSinkIdentifier {
	public static final String VERSION_IDENTIFIER = "vjdbc-version";
	public static final String METHOD_IDENTIFIER = "vjdbc-method";
	public static final String V2_METHOD_IDENTIFIER = "vjdbc2-method";
	
	public static final String PROTOCOL_KRYO = "k";

    public static final String CONNECT_COMMAND = "connect";
    public static final String PROCESS_COMMAND = "process";
}
