package de.simplicit.vjdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;

public class InflatingInput extends Input {

	private boolean streamInitialized = false;
	
	public InflatingInput(InputStream inputStream, int bufferSize) {
		super(inputStream, bufferSize);
	}

	public InflatingInput(InputStream inputStream) {
		super(inputStream);
	}

	@Override
	protected int fill(byte[] buffer, int offset, int count) throws KryoException {
		if (!streamInitialized){
			int header;
			try {
				header = inputStream.read();
			} catch (IOException e) {
				throw new KryoException(e);
			}			
			if (header==DeflatingOutput.DEFLATED_HEADER){
				inputStream = new InflaterInputStream(inputStream);
			} else if (header!=DeflatingOutput.CLEAR_HEADER){
				throw new KryoException("Invalid stream header");
			}
			
			streamInitialized = true;
		}		
		return super.fill(buffer, offset, count);
	}
}
