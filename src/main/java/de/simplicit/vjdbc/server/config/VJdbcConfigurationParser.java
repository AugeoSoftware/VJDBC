package de.simplicit.vjdbc.server.config;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.substitution.MultiVariableExpander;
import org.apache.commons.digester.substitution.VariableSubstitutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class VJdbcConfigurationParser {

    private static final Log _logger = LogFactory.getLog(VJdbcConfigurationParser.class);


    /**
     * Initialization with pre-opened InputStream.
     * @param configResourceInputStream InputStream
     * @throws ConfigurationException
     */
    public static VJdbcConfiguration parse(InputStream configResourceInputStream, Properties configVariables) throws ConfigurationException {
            try {
            	VJdbcConfiguration configuration = new VJdbcConfiguration();
                Digester digester = createDigester(configuration, configVariables);
                digester.parse(configResourceInputStream);
                configuration.validateConnections();;
                if(_logger.isInfoEnabled()) {
                	configuration.log();
                }
                return configuration;
            } catch(Exception e) {
                String msg = "VJdbc-Configuration failed";
                _logger.error(msg, e);
                throw new ConfigurationException(msg, e);
            }        
    }


    /**
     * Initialization with resource.
     * @param configResource Resource to be loaded by the ClassLoader
     * @throws ConfigurationException
     */
    public static VJdbcConfiguration parse(String configResource, Properties configVariables) throws ConfigurationException {

            try {
            	VJdbcConfiguration configuration = new VJdbcConfiguration();
                Digester digester = createDigester(configuration, configVariables);
                digester.parse(configResource);
                configuration.validateConnections();            	
                if(_logger.isInfoEnabled()) {
                	configuration.log();
                }
                return configuration;
            } catch(Exception e) {
                String msg = "VJdbc-Configuration failed";
                _logger.error(msg, e);
                throw new ConfigurationException(msg, e);
            }
    }    
    
	private static Digester createDigester(VJdbcConfiguration configuration, Properties vars) {
	    Digester digester = createDigester(configuration);
	
	    if(vars != null) {
	        MultiVariableExpander expander = new MultiVariableExpander();
	        expander.addSource("$", vars);
	        digester.setSubstitutor(new VariableSubstitutor(expander));
	    }
	
	    return digester;
	}
		
	
	private static Digester createDigester(VJdbcConfiguration configuration) {
	    Digester digester = new Digester();
	
	    digester.push(configuration);
	
	    digester.addObjectCreate("vjdbc-configuration/occt", DigesterOcctConfiguration.class);
	    digester.addSetProperties("vjdbc-configuration/occt");
	    digester.addSetNext("vjdbc-configuration/occt",
	            "setOcctConfiguration",
	            OcctConfiguration.class.getName());
	
	    digester.addObjectCreate("vjdbc-configuration/rmi", DigesterRmiConfiguration.class);
	    digester.addSetProperties("vjdbc-configuration/rmi");
	    digester.addSetNext("vjdbc-configuration/rmi",
	            "setRmiConfiguration",
	            RmiConfiguration.class.getName());
	
	    digester.addObjectCreate("vjdbc-configuration/connection", DigesterConnectionConfiguration.class);
	    digester.addSetProperties("vjdbc-configuration/connection");
	    digester.addSetNext("vjdbc-configuration/connection",
	            "addConnection",
	            ConnectionConfiguration.class.getName());
	
	    digester.addObjectCreate("vjdbc-configuration/connection/connection-pool", ConnectionPoolConfiguration.class);
	    digester.addSetProperties("vjdbc-configuration/connection/connection-pool");
	    digester.addSetNext("vjdbc-configuration/connection/connection-pool",
	            "setConnectionPoolConfiguration",
	            ConnectionPoolConfiguration.class.getName());
	
	    // Named-Queries
	    digester.addObjectCreate("vjdbc-configuration/connection/named-queries", NamedQueryConfiguration.class);
	    digester.addCallMethod("vjdbc-configuration/connection/named-queries/entry", "addEntry", 2);
	    digester.addCallParam("vjdbc-configuration/connection/named-queries/entry", 0, "id");
	    digester.addCallParam("vjdbc-configuration/connection/named-queries/entry", 1);
	    digester.addSetNext("vjdbc-configuration/connection/named-queries",
	            "setNamedQueries",
	            NamedQueryConfiguration.class.getName());
	
	    // Query-Filters
	    digester.addObjectCreate("vjdbc-configuration/connection/query-filters", QueryFilterConfiguration.class);
	    digester.addCallMethod("vjdbc-configuration/connection/query-filters/deny", "addDenyEntry", 2);
	    digester.addCallParam("vjdbc-configuration/connection/query-filters/deny", 0);
	    digester.addCallParam("vjdbc-configuration/connection/query-filters/deny", 1, "type");
	    digester.addCallMethod("vjdbc-configuration/connection/query-filters/allow", "addAllowEntry", 2);
	    digester.addCallParam("vjdbc-configuration/connection/query-filters/allow", 0);
	    digester.addCallParam("vjdbc-configuration/connection/query-filters/allow", 1, "type");
	    digester.addSetNext("vjdbc-configuration/connection/query-filters",
	            "setQueryFilters",
	            QueryFilterConfiguration.class.getName());
	
	    return digester;
	}


}
