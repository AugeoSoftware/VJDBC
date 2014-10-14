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
	private static String getMajorVersion(String versionValue) {
		if (versionValue==null || "".equals(versionValue)){
			return UNKNOWN;
		}
		
		int i = versionValue.indexOf('.');
		if (i>=0){
			return versionValue.substring(0, i);
		}
		return versionValue;
	}
	private static String getMinorVersion(String versionValue) {
		if (versionValue==null || "".equals(versionValue)){
			return UNKNOWN;
		}
		int i = versionValue.indexOf('.');
		if (i>=0){
			int k = versionValue.indexOf('.', i+1);
			if (k>=0) {
				return versionValue.substring(i+1, k);
			} else {
				return versionValue.substring(i+1);
			}
		}
		return "";
	}

	private static int parseInt(String s){
		try {
			return Integer.parseInt(s);
		} catch (NumberFormatException e){
			return 0;
		}
	}
	
	public static final String version = getVersion(); 
	
	public static final String majorVersion = getMajorVersion(version);
	
	public static final String minorVersion = getMinorVersion(version);

	public static final int majorIntVersion = parseInt(majorVersion);
	
	public static final int minorIntVersion = parseInt(minorVersion);	
	
}
