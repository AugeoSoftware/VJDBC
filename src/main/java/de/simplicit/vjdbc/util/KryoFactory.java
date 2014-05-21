package de.simplicit.vjdbc.util;

import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;

import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.FlattenedColumnValues;
import de.simplicit.vjdbc.serial.FlattenedColumnValuesSerializer;
import de.simplicit.vjdbc.serial.RowPacket;
import de.simplicit.vjdbc.serial.RowPacketSerializer;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.serial.UIDExSerializer;

public class KryoFactory {

	private static final RowPacketSerializer ROW_PACKET_SERIALIZER = new RowPacketSerializer();
	private static final FlattenedColumnValuesSerializer FLATTENED_COLUMN_VALUES_SERIALIZER = new FlattenedColumnValuesSerializer();
	private static final UIDExSerializer UIDEX_SERIALIZER = new UIDExSerializer();

	/**
	 * Instance holder see {@linkplain http://en.wikipedia.org/wiki/Singleton_pattern#The_solution_of_Bill_Pugh} for details.
	 *
	 */
	private static class Instance {
		private static final KryoFactory instance = new KryoFactory();
	}
	
	
	public static KryoFactory getInstance() {
		return Instance.instance;
	}
	
	private KryoFactory() {
		// TODO	
	}
	
	private Kryo createKryo() {
		Kryo kryo = new Kryo();
		kryo.register(SQLException.class);
		kryo.register(UIDEx.class, UIDEX_SERIALIZER);
		kryo.register(CallingContext.class /** TODO new CallingContextSerializer */);
		kryo.register(RowPacket.class, ROW_PACKET_SERIALIZER);
		kryo.register(FlattenedColumnValues.class, FLATTENED_COLUMN_VALUES_SERIALIZER);
		return kryo;
	}
	
	public Kryo getKryo() {
		// TODO set up a pool of kryo objects
		return createKryo();
	}
	
	public void releaseKryo(Kryo kryo){
		// TODO 
	}	
}
