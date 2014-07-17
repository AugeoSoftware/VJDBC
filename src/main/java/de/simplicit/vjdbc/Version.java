package de.simplicit.vjdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Version {

	private static final String UNKNOWN = "UNKNOWN";
	
	private static String getVersion() {
		String path = "version.prop";
		InputStream stream = Version.class.getResourceAsStream(path);
		if (stream == null){
			return UNKNOWN;
		}
		Properties props = new Properties();
		try {
			props.load(stream);
			return (String) props.get("version");
		} catch (IOException e) {
			return UNKNOWN;
		} finally {
			try {
				stream.close();
			} catch (IOException e) {
			}
		}
	}

	public static final String version = getVersion(); 
	
}
