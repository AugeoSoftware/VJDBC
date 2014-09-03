package de.simplicit.vjdbc.util;

import java.util.zip.Deflater;

import de.simplicit.vjdbc.server.config.ConfigurationException;
/**
 * Utility class for parsing performance parameters: compression mode, compression threshold, row packet size.
 * Provides means for: parsing from string, validating integer value, 
 * encoding/decoding parameters into single integer, aka performance profile.  
 * 
 * @author semenov
 *
 */
public class PerformanceConfig {

	
	public static final int MIN_COMPRESSION_THRESHOLD = 0;
	public static final int MAX_COMPRESSION_THRESHOLD = 0x00000FFF;
	public static final int MIN_ROW_PACKET_SIZE = 1;
	public static final int MAX_ROW_PACKET_SIZE = 0x00000FFFF;	
	
	
	public static int parseCompressionMode(String value) throws ConfigurationException {
        if(value.equalsIgnoreCase("bestspeed")) {
            return Deflater.BEST_SPEED;
        } else if(value.equalsIgnoreCase("bestcompression")) {
            return Deflater.BEST_COMPRESSION;
        } else if(value.equalsIgnoreCase("none")) {
            return Deflater.NO_COMPRESSION;
        }
		try {
			return validateCompressionMode(Integer.parseInt(value));
		} catch (NumberFormatException e) {
    		throw new ConfigurationException("Invalid compression mode value: "+value+" should be in ["+Deflater.NO_COMPRESSION+".."+Deflater.BEST_COMPRESSION+"]", e);
		}
	}
	
	public static int validateCompressionMode(int value) throws ConfigurationException {
    	if (value<Deflater.NO_COMPRESSION || value>Deflater.BEST_COMPRESSION){
    		throw new ConfigurationException("Invalid compression mode value: "+value+" should be in ["+Deflater.NO_COMPRESSION+".."+Deflater.BEST_COMPRESSION+"]");
    	}
    	return value;
	}
	
	
	public static int parseCompressionThreshold(String value) throws ConfigurationException {
		try {
			return validateCompressionThreshold(Integer.parseInt(value));
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Invalid compression threshold: "+value+" should be in ["+MIN_COMPRESSION_THRESHOLD+".."+MAX_COMPRESSION_THRESHOLD+"]", e);
		}
	}
	
	public static int validateCompressionThreshold(int value) throws ConfigurationException {
		if (value<MIN_COMPRESSION_THRESHOLD || value>MAX_COMPRESSION_THRESHOLD){
			throw new ConfigurationException("Invalid compression threshold: "+value+" should be in ["+MIN_COMPRESSION_THRESHOLD+".."+MAX_COMPRESSION_THRESHOLD+"]");
		}
		return value;
	}
	
	public static int parseRowPacketSize(String value) throws ConfigurationException {
		try {
			return validateRowPacketSize(Integer.parseInt(value));
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Invalid row packet size: "+value+" should be in ["+MIN_ROW_PACKET_SIZE+".."+MAX_ROW_PACKET_SIZE+"]", e);
		}		
	}
	
	public static int validateRowPacketSize(int value) throws ConfigurationException {
		if (value<MIN_ROW_PACKET_SIZE || value>MAX_ROW_PACKET_SIZE){
			throw new ConfigurationException("Invalid row packet size: "+value+" should be in ["+MIN_ROW_PACKET_SIZE+".."+MAX_ROW_PACKET_SIZE+"]");
		}
		return value;		
	}
	
	public static int getPerformanceProfile(int compressionMode, int compressionThreshold, int rowPacketSize){		
		return ((rowPacketSize&MAX_ROW_PACKET_SIZE)<<16) | ((compressionMode&0xF)<<12) | (compressionThreshold&MAX_COMPRESSION_THRESHOLD);
	}

	public static int getCompressionMode(int performanceProfile) throws ConfigurationException {
		return validateCompressionMode((performanceProfile>>12)&0xF);
	}
	
	public static int getCompressionThreshold(int performanceProfile){
		return performanceProfile&MAX_COMPRESSION_THRESHOLD;
	}
	
	public static int getRowPacketSize(int performanceProfile){
		return (performanceProfile>>16)&MAX_ROW_PACKET_SIZE;
	}
}
