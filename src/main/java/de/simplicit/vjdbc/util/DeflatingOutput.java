package de.simplicit.vjdbc.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;

public class DeflatingOutput extends Output {
	/** the mark, indicating that the rest of the stream is compressed */
	public static final int DEFLATED_HEADER = 0xDE;
	/** the mark, indicating that the rest of the stream is not compressed */
	public static final int CLEAR_HEADER = 0xC1;
	
	private boolean streamInitialized = false;
	private int compressionMode = Deflater.NO_COMPRESSION;
	private int threshold = 0;
	
	
	public DeflatingOutput(OutputStream outputStream) {
		super(outputStream);
	}

	public DeflatingOutput(OutputStream outputStream, int bufferSize) {
		super(outputStream, bufferSize);
	}

	public DeflatingOutput(OutputStream outputStream, int compressionMode, int threshold) {
		super(outputStream);
		this.compressionMode = compressionMode;
		this.threshold = threshold;
	}

	public DeflatingOutput(OutputStream outputStream, int bufferSize, int compressionMode, int threshold) {
		super(outputStream, bufferSize);
		this.compressionMode = compressionMode;
		this.threshold = threshold;
	}

	@Override
	public void flush() throws KryoException {
		if (!streamInitialized){
			try {
				if (compressionMode==Deflater.NO_COMPRESSION || position()<=threshold){
					outputStream.write(CLEAR_HEADER);
				} else {
					outputStream.write(DEFLATED_HEADER);
					Deflater deflater = new Deflater(compressionMode);
					outputStream = new DeflaterOutputStream(outputStream, deflater);
				}			
			} catch (IOException e) {
				throw new KryoException(e);
			}
			streamInitialized = true;
		}
		super.flush();
	}
	
}
