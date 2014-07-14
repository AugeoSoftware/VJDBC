// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.server.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.zip.Deflater;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.simplicit.vjdbc.VJdbcProperties;
import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.server.command.CommandProcessor;
import de.simplicit.vjdbc.server.config.ConfigurationException;
import de.simplicit.vjdbc.server.config.ConnectionConfiguration;
import de.simplicit.vjdbc.server.config.VJdbcConfiguration;
import de.simplicit.vjdbc.servlet.ServletCommandSinkIdentifier;
import de.simplicit.vjdbc.util.DeflatingOutput;
import de.simplicit.vjdbc.util.InflatingInput;
import de.simplicit.vjdbc.util.KryoFactory;
import de.simplicit.vjdbc.util.SQLExceptionHelper;
import de.simplicit.vjdbc.util.StreamCloser;
import javax.servlet.ServletContext;

public class KryoServletCommandSink extends HttpServlet {
    private static final String INIT_PARAMETER_CONFIG_RESOURCE = "config-resource";
    private static final String INIT_PARAMETER_CONFIG_VARIABLES = "config-variables";
    private static final String DEFAULT_CONFIG_RESOURCE = "/WEB-INF/vjdbc-config.xml";
    private static final long serialVersionUID = 3257570624301249846L;
    private static Log _logger = LogFactory.getLog(KryoServletCommandSink.class);

    private CommandProcessor _processor;

    public KryoServletCommandSink() {
    }

    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);

        String configResource = servletConfig.getInitParameter(INIT_PARAMETER_CONFIG_RESOURCE);

        // Use default location when nothing is configured
        if(configResource == null) {
            configResource = DEFAULT_CONFIG_RESOURCE;
        }

        ServletContext ctx = servletConfig.getServletContext();

        _logger.info("Trying to get config resource " + configResource + "...");
        InputStream configResourceInputStream = ctx.getResourceAsStream(configResource);
        if(null == configResourceInputStream) {
            try {
                configResourceInputStream =
                    new FileInputStream(ctx.getRealPath(configResource));
            } catch (FileNotFoundException fnfe) {
            }
        }

        if(configResourceInputStream == null) {
        	try {
        		// check if configuration is already initialized
        		VJdbcConfiguration.singleton(); // throws RuntimeException if not initialized 
        	} catch (RuntimeException e){
        		String msg = "VJDBC-Configuration " + configResource + " not found !";
        		_logger.error(msg);
        		throw new ServletException(msg);
        	}
        } else {

	        // Are config variables specifiec ?
	        String configVariables = servletConfig.getInitParameter(INIT_PARAMETER_CONFIG_VARIABLES);
	        Properties configVariablesProps = null;
	
	        if(configVariables != null) {
	            _logger.info("... using variables specified in " + configVariables);
	
	            InputStream configVariablesInputStream = null;
	
	            try {
	                configVariablesInputStream = ctx.getResourceAsStream(configVariables);
	                if(null == configVariablesInputStream) {
	                    configVariablesInputStream =
	                        new FileInputStream(ctx.getRealPath(configVariables));
	                }
	
	                if(configVariablesInputStream == null) {
	                    String msg = "Configuration-Variables " + configVariables + " not found !";
	                    _logger.error(msg);
	                    throw new ServletException(msg);
	                }
	
	                configVariablesProps = new Properties();
	                configVariablesProps.load(configVariablesInputStream);
	            } catch (IOException e) {
	                String msg = "Reading of configuration variables failed";
	                _logger.error(msg, e);
	                throw new ServletException(msg, e);
	            } finally {
	                if(configVariablesInputStream != null) {
	                    try {
	                        configVariablesInputStream.close();
	                    } catch (IOException e) {}
	                }
	            }
	        }
	
	        try {
	            _logger.info("Initialize VJDBC-Configuration");
	            VJdbcConfiguration.init(configResourceInputStream, configVariablesProps);
	        } catch (ConfigurationException e) {
	            _logger.error("Initialization failed", e);
	            throw new ServletException("VJDBC-Initialization failed", e);
	        } finally {
	                StreamCloser.close(configResourceInputStream);
	        }
        }
        _processor = CommandProcessor.getInstance();
    }

    public void destroy() {
    }

    protected void doGet(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException {
        handleRequest(httpServletRequest, httpServletResponse);
    }

    protected void doPost(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException {
        handleRequest(httpServletRequest, httpServletResponse);
    }

    private void handleRequest(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws ServletException {
        Input input = null;
        Output output = null;
        Kryo kryo = null;
        try {
            // Get the method to execute
            String method = httpServletRequest.getHeader(ServletCommandSinkIdentifier.METHOD_IDENTIFIER);

            if(method != null) {
            	kryo = KryoFactory.getInstance().getKryo();
                //ois = new ObjectInputStream(httpServletRequest.getInputStream());
            	input = new InflatingInput(httpServletRequest.getInputStream());
                // And initialize the output
                OutputStream os = httpServletResponse.getOutputStream();
                //oos = new ObjectOutputStream(os);
                output = new DeflatingOutput(os); // TODO pass compression parameters
                Object objectToReturn = null;

                try {
                    // Some command to process ?
                    if(method.equals(ServletCommandSinkIdentifier.PROCESS_COMMAND)) {
                        // Read parameter objects
                    	Long connuid = kryo.readObjectOrNull(input, Long.class);
                    	Long uid = kryo.readObjectOrNull(input, Long.class);
                    	Command cmd = (Command) kryo.readClassAndObject(input);
                    	CallingContext ctx = kryo.readObjectOrNull(input, CallingContext.class);
                        // Delegate execution to the CommandProcessor
                        objectToReturn = _processor.process(connuid, uid, cmd, ctx);
                    } else if(method.equals(ServletCommandSinkIdentifier.CONNECT_COMMAND)) {
                        if (_logger.isDebugEnabled()){
                        	_logger.debug("Connection request from "+httpServletRequest.getRemoteAddr());
                        }
                        String url = kryo.readObject(input, String.class);
                        Properties props = kryo.readObject(input, Properties.class);
                        Properties clientInfo = kryo.readObject(input, Properties.class);
                    	CallingContext ctx = kryo.readObjectOrNull(input, CallingContext.class);

                        ConnectionConfiguration connectionConfiguration = VJdbcConfiguration.singleton().getConnection(url);

                        if(connectionConfiguration != null) {
                            Connection conn = connectionConfiguration.create(props);
                            Object userName = props.get(VJdbcProperties.USER_NAME);
							if (userName!=null){
								clientInfo.put(VJdbcProperties.USER_NAME, userName);
							}
                            objectToReturn = _processor.registerConnection(conn, connectionConfiguration, clientInfo, ctx);
                        } else {
                            objectToReturn = new SQLException("VJDBC-Connection " + url + " not found");
                        }
                    }
                } catch (Throwable t) {
                    // Wrap any exception so that it can be transported back to
                    // the client
                    objectToReturn = SQLExceptionHelper.wrap(t);
                }

                // Write the result in the response buffer
                kryo.writeClassAndObject(output, objectToReturn);
                output.flush();

                httpServletResponse.flushBuffer();
            } else {
                // No VJDBC-Method ? Then we redirect the stupid browser user to
                // some information page :-)
                httpServletResponse.sendRedirect("index.html");
            }
        } catch (Exception e) {
            _logger.error("Unexpected Exception", e);
            throw new ServletException(e);
        } finally {
            StreamCloser.close(input);
            StreamCloser.close(output);
            if (kryo!=null){
            	KryoFactory.getInstance().releaseKryo(kryo);
            }
        }
    }
}
