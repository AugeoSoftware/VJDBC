// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.servlet.kryo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.sql.SQLException;
import java.util.Properties;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.servlet.AbstractServletCommandSinkClient;
import de.simplicit.vjdbc.servlet.RequestEnhancer;
import de.simplicit.vjdbc.servlet.ServletCommandSinkIdentifier;
import de.simplicit.vjdbc.util.KryoFactory;
import de.simplicit.vjdbc.util.SQLExceptionHelper;
import de.simplicit.vjdbc.util.StreamCloser;

public class KryoServletCommandSinkJdkHttpClient extends AbstractServletCommandSinkClient {
    
	private final Kryo kryo;
	
	public KryoServletCommandSinkJdkHttpClient(String url, RequestEnhancer requestEnhancer) throws SQLException {
        super(url, requestEnhancer);
        kryo = KryoFactory.getInstance().getKryo(); 
    }

    public UIDEx connect(String database, Properties props, Properties clientInfo, CallingContext ctx) throws SQLException {
        HttpURLConnection conn = null;

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
            conn.setRequestProperty("Content-type", "binary/x-java-serialized" );
            conn.setRequestProperty(ServletCommandSinkIdentifier.METHOD_IDENTIFIER,
                                    ServletCommandSinkIdentifier.CONNECT_COMMAND);
            // Finally let the optional Request-Enhancer set request properties
            if(_requestEnhancer != null) {
                _requestEnhancer.enhanceConnectRequest(new KryoRequestModifier(conn));
            }

            // Write the parameter objects to the ObjectOutputStream
            output = new Output(conn.getOutputStream());
            kryo.writeObject(output, database);
            kryo.writeObject(output, props);
            kryo.writeObject(output, clientInfo);
            kryo.writeObjectOrNull(output, ctx, CallingContext.class);
            output.flush();
            
            // Connect ...
            conn.connect();
            // Read the result object from the InputStream
            
            input = new Input(conn.getInputStream());
            Object result = kryo.readClassAndObject(input);
            
//            // This might be a SQLException which must be rethrown
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
        }
    }

    public Object process(Long connuid, Long uid, Command cmd, CallingContext ctx) throws SQLException {
        HttpURLConnection conn = null;
        Input input = null;
        Output output = null;
        
        try {
            conn = (HttpURLConnection)_url.openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty(ServletCommandSinkIdentifier.METHOD_IDENTIFIER, ServletCommandSinkIdentifier.PROCESS_COMMAND);
            // Finally let the optional Request-Enhancer set request properties
            if(_requestEnhancer != null) {
                _requestEnhancer.enhanceProcessRequest(new KryoRequestModifier(conn));
            }
            conn.connect();
            output = new Output(conn.getOutputStream());
            kryo.writeObjectOrNull(output, connuid, Long.class);
            kryo.writeObjectOrNull(output, uid, Long.class);
            kryo.writeClassAndObject(output, cmd);
            kryo.writeObjectOrNull(output, ctx, CallingContext.class);
            output.flush();

            input = new Input(conn.getInputStream());
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
        }
    }

	@Override
	public void close() {
		KryoFactory.getInstance().releaseKryo(kryo);
		super.close();		
	}
    
    
    
}
