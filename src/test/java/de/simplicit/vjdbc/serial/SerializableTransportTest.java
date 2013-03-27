package de.simplicit.vjdbc.serial;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.Deflater;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SerializableTransportTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}


	private final static String LONG_STRING = "LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING LONG_STRING ";
	
	
	private SerializableTransport transfer(SerializableTransport in) throws IOException, ClassNotFoundException{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(in);
		oos.close();
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);
		return (SerializableTransport) ois.readObject();
	}
	
	
	private void testTrasport(Object payload, int compressionMode, int minSize) throws IOException, ClassNotFoundException{
		SerializableTransport in = new SerializableTransport(payload, compressionMode, minSize);

		assertEquals(payload, in.getTransportee());
		
		SerializableTransport out = transfer(in);
		
		assertEquals(payload, out.getTransportee());
	}
	
	@Test
	public void testSerializableTransportObjectIntLong0() throws IOException, ClassNotFoundException {
		testTrasport("payload string", Deflater.NO_COMPRESSION, 2000);
	}
	@Test
	public void testSerializableTransportObjectIntLong1() throws IOException, ClassNotFoundException {
	
		testTrasport("payload string", Deflater.BEST_SPEED, 2000);
	}
	@Test
	public void testSerializableTransportObjectIntLong2() throws IOException, ClassNotFoundException {

		testTrasport("payload string", Deflater.BEST_COMPRESSION, 2000);
	}
	@Test
	public void testSerializableTransportObjectIntLong3() throws IOException, ClassNotFoundException {
	
		testTrasport(LONG_STRING, Deflater.NO_COMPRESSION, 2000);
	}
	@Test
	public void testSerializableTransportObjectIntLong4() throws IOException, ClassNotFoundException {

		testTrasport(LONG_STRING, Deflater.BEST_SPEED, 2000);
	}
	@Test
	public void testSerializableTransportObjectIntLong5() throws IOException, ClassNotFoundException {

		testTrasport(LONG_STRING, Deflater.BEST_COMPRESSION, 2000);
		
	}
}
