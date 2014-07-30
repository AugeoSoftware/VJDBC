// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.servlet.kryo;

import static de.simplicit.vjdbc.servlet.ServletCommandSinkIdentifier.CONNECT_COMMAND;
import static de.simplicit.vjdbc.servlet.ServletCommandSinkIdentifier.PROCESS_COMMAND;
import static de.simplicit.vjdbc.servlet.ServletCommandSinkIdentifier.PROTOCOL_KRYO;
import static de.simplicit.vjdbc.servlet.ServletCommandSinkIdentifier.V2_METHOD_IDENTIFIER;
import static de.simplicit.vjdbc.servlet.ServletCommandSinkIdentifier.VERSION_IDENTIFIER;

import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.Properties;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.Version;
import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.servlet.AbstractServletCommandSinkClient;
import de.simplicit.vjdbc.servlet.RequestEnhancer;
import de.simplicit.vjdbc.util.DeflatingOutput;
import de.simplicit.vjdbc.util.InflatingInput;
import de.simplicit.vjdbc.util.KryoFactory;
import de.simplicit.vjdbc.util.SQLExceptionHelper;
import de.simplicit.vjdbc.util.StreamCloser;

public class KryoServletCommandSinkJdkHttpClient extends AbstractServletCommandSinkClient {
    
	private static final String PROTOCOL_VERSION = Version.version+PROTOCOL_KRYO;
	
	public KryoServletCommandSinkJdkHttpClient(String url, RequestEnhancer requestEnhancer) throws SQLException {
        super(url, requestEnhancer);
    }

    public UIDEx connect(String database, Properties props, Properties clientInfo, CallingContext ctx) throws SQLException {
        HttpURLConnection conn = null;
        Kryo kryo = null;
        Input input = null;
        Output output = null;
        try {
            // Open connection and adjust the Input/Output
            conn = (HttpURLConnection)_url.openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod("POST");
            conn.setAllowUserInteraction(false); // system may not ask the user
            conn.setUseCaches(false);
            conn.setInstanceFollowRedirects(false);
            conn.setRequestProperty("Content-type", "binary/x-java-serialized" );
            conn.setRequestProperty(V2_METHOD_IDENTIFIER, CONNECT_COMMAND);
            conn.setRequestProperty(VERSION_IDENTIFIER, PROTOCOL_VERSION);
            // Finally let the optional Request-Enhancer set request properties
            if(_requestEnhancer != null) {
                _requestEnhancer.enhanceConnectRequest(new KryoRequestModifier(conn));
            }

            
            // Write the parameter objects to the ObjectOutputStream
            kryo = KryoFactory.getInstance().getKryo();
            output = new DeflatingOutput(conn.getOutputStream());
            kryo.writeObject(output, database);
            kryo.writeObject(output, props);
            kryo.writeObject(output, clientInfo);
            kryo.writeObjectOrNull(output, ctx, CallingContext.class);
            output.flush();
            
            // Connect ...
            conn.connect();
            // check the response
            int responseCode = conn.getResponseCode();
            switch (responseCode){
            case HttpURLConnection.HTTP_MOVED_TEMP: // response from legacy 1.x version, caused by change in ServletCommandSinkIdentifier.METHOD_IDENTIFIER
            	throw new SQLException("The client VJDBC driver version "+PROTOCOL_VERSION+" is not compatible with the server version 1.x");
            case HttpURLConnection.HTTP_VERSION: // response from version 2.0+
            	throw new SQLException("The client VJDBC driver version "+PROTOCOL_VERSION+" is not compatible with the server version "+conn.getHeaderField(VERSION_IDENTIFIER));
            case HttpURLConnection.HTTP_OK:
            	break;
            default:
            	throw new SQLException("Unexpected server response: "+responseCode+" "+conn.getResponseMessage());
            }
            
            // Read the result object from the InputStream            
            input = new InflatingInput(conn.getInputStream());
            Object result = kryo.readClassAndObject(input);
            
            // This might be a SQLException which must be rethrown
            if(result instanceof SQLException) {
                throw (SQLException)result;
            }
            else {
                return (UIDEx)result;
            }
        } catch(SQLException e) {
            throw e;
        } catch(Exception e) {
            throw SQLExceptionHelper.wrap(e);
        } finally {
            // Cleanup resources
            StreamCloser.close(input);
            StreamCloser.close(output);

            if(conn != null) {
                conn.disconnect();
            }
            if (kryo!=null){
            	KryoFactory.getInstance().releaseKryo(kryo);
            }
        }
    }

    public Object process(Long connuid, Long uid, Command cmd, CallingContext ctx) throws SQLException {
        HttpURLConnection conn = null;
        Kryo kryo = null;
        Input input = null;
        Output output = null;
        
        try {
            conn = (HttpURLConnection)_url.openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty(VERSION_IDENTIFIER, PROTOCOL_VERSION);
            conn.setRequestProperty(V2_METHOD_IDENTIFIER, PROCESS_COMMAND);
            // Finally let the optional Request-Enhancer set request properties
            if(_requestEnhancer != null) {
                _requestEnhancer.enhanceProcessRequest(new KryoRequestModifier(conn));
            }
            conn.connect();
            kryo = KryoFactory.getInstance().getKryo();
            output = new DeflatingOutput(conn.getOutputStream());
            kryo.writeObjectOrNull(output, connuid, Long.class);
            kryo.writeObjectOrNull(output, uid, Long.class);
            kryo.writeClassAndObject(output, cmd);
            kryo.writeObjectOrNull(output, ctx, CallingContext.class);
            output.flush();

            input = new InflatingInput(conn.getInputStream());
            Object result = kryo.readClassAndObject(input);
            if(result instanceof SQLException) {
                throw (SQLException)result;
            }
            else {
                return result;
            }
        } catch(SQLException e) {
            throw e;
        } catch(Exception e) {
            throw SQLExceptionHelper.wrap(e);
        } finally {
            // Cleanup resources
            StreamCloser.close(input);
            StreamCloser.close(output);

            if(conn != null) {
                conn.disconnect();
            }
            
            if (kryo!=null){
            	KryoFactory.getInstance().releaseKryo(kryo);
            }
        }
    }
}
