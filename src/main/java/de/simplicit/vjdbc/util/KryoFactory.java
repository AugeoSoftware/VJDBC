package de.simplicit.vjdbc.util;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.esotericsoftware.kryo.Kryo;

import de.simplicit.vjdbc.VJdbcException;
import de.simplicit.vjdbc.command.CallableStatementGetArrayCommand;
import de.simplicit.vjdbc.command.CallableStatementGetBlobCommand;
import de.simplicit.vjdbc.command.CallableStatementGetCharacterStreamCommand;
import de.simplicit.vjdbc.command.CallableStatementGetClobCommand;
import de.simplicit.vjdbc.command.CallableStatementGetNCharacterStreamCommand;
import de.simplicit.vjdbc.command.CallableStatementGetNClobCommand;
import de.simplicit.vjdbc.command.CallableStatementGetObjectCommand;
import de.simplicit.vjdbc.command.CallableStatementGetRefCommand;
import de.simplicit.vjdbc.command.CallableStatementGetSQLXMLCommand;
import de.simplicit.vjdbc.command.CallableStatementSetAsciiStreamCommand;
import de.simplicit.vjdbc.command.CallableStatementSetBinaryStreamCommand;
import de.simplicit.vjdbc.command.CallableStatementSetBlobCommand;
import de.simplicit.vjdbc.command.CallableStatementSetCharacterStreamCommand;
import de.simplicit.vjdbc.command.CallableStatementSetClobCommand;
import de.simplicit.vjdbc.command.CallableStatementSetNCharacterStreamCommand;
import de.simplicit.vjdbc.command.CallableStatementSetNClobCommand;
import de.simplicit.vjdbc.command.CallableStatementSetObjectCommand;
import de.simplicit.vjdbc.command.CallableStatementSetRowIdCommand;
import de.simplicit.vjdbc.command.CallableStatementSetSQLXMLCommand;
import de.simplicit.vjdbc.command.Command;
import de.simplicit.vjdbc.command.ConnectionCommitCommand;
import de.simplicit.vjdbc.command.ConnectionPrepareCallCommand;
import de.simplicit.vjdbc.command.ConnectionPrepareStatementCommand;
import de.simplicit.vjdbc.command.ConnectionPrepareStatementExtendedCommand;
import de.simplicit.vjdbc.command.ConnectionReleaseSavepointCommand;
import de.simplicit.vjdbc.command.ConnectionRollbackWithSavepointCommand;
import de.simplicit.vjdbc.command.ConnectionSetClientInfoCommand;
import de.simplicit.vjdbc.command.DatabaseMetaDataGetUserNameCommand;
import de.simplicit.vjdbc.command.DestroyCommand;
import de.simplicit.vjdbc.command.NextRowPacketCommand;
import de.simplicit.vjdbc.command.PingCommand;
import de.simplicit.vjdbc.command.PreparedStatementExecuteBatchCommand;
import de.simplicit.vjdbc.command.PreparedStatementExecuteCommand;
import de.simplicit.vjdbc.command.PreparedStatementQueryCommand;
import de.simplicit.vjdbc.command.PreparedStatementUpdateCommand;
import de.simplicit.vjdbc.command.ReflectiveCommand;
import de.simplicit.vjdbc.command.ResultSetGetMetaDataCommand;
import de.simplicit.vjdbc.command.ResultSetProducerCommand;
import de.simplicit.vjdbc.command.StatementCancelCommand;
import de.simplicit.vjdbc.command.StatementExecuteBatchCommand;
import de.simplicit.vjdbc.command.StatementExecuteCommand;
import de.simplicit.vjdbc.command.StatementExecuteExtendedCommand;
import de.simplicit.vjdbc.command.StatementGetGeneratedKeysCommand;
import de.simplicit.vjdbc.command.StatementGetResultSetCommand;
import de.simplicit.vjdbc.command.StatementQueryCommand;
import de.simplicit.vjdbc.command.StatementUpdateCommand;
import de.simplicit.vjdbc.command.StatementUpdateExtendedCommand;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.CallingContextSerializer;
import de.simplicit.vjdbc.serial.RowPacket;
import de.simplicit.vjdbc.serial.RowPacketSerializer;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.serial.UIDExSerializer;

public class KryoFactory {

	private static final RowPacketSerializer ROW_PACKET_SERIALIZER = new RowPacketSerializer();
	private static final UIDExSerializer UIDEX_SERIALIZER = new UIDExSerializer();
	private static final CallingContextSerializer CALLING_CONTEXT_SERIALIZER = new CallingContextSerializer();
	
	private final ConcurrentLinkedQueue<Kryo> kryoCache = new ConcurrentLinkedQueue<Kryo>();
	

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
	}
	
	private Kryo createKryo() {
		Kryo kryo = new Kryo();
		kryo.register(Properties.class);
		kryo.register(SQLException.class);
		kryo.register(VJdbcException.class);
		kryo.register(UIDEx.class, UIDEX_SERIALIZER);
		kryo.register(CallingContext.class, CALLING_CONTEXT_SERIALIZER);
		kryo.register(RowPacket.class, ROW_PACKET_SERIALIZER);
		
		// Commands
		kryo.register(CallableStatementGetArrayCommand.class);
		kryo.register(CallableStatementGetBlobCommand.class);
		kryo.register(CallableStatementGetCharacterStreamCommand.class);
		kryo.register(CallableStatementGetClobCommand.class);
		kryo.register(CallableStatementGetNCharacterStreamCommand.class);
		kryo.register(CallableStatementGetNClobCommand.class);
		kryo.register(CallableStatementGetObjectCommand.class);
		kryo.register(CallableStatementGetRefCommand.class);
		kryo.register(CallableStatementGetSQLXMLCommand.class);
		kryo.register(CallableStatementSetAsciiStreamCommand.class);
		kryo.register(CallableStatementSetBinaryStreamCommand.class);
		kryo.register(CallableStatementSetBlobCommand.class);
		kryo.register(CallableStatementSetCharacterStreamCommand.class);
		kryo.register(CallableStatementSetClobCommand.class);
		kryo.register(CallableStatementSetNCharacterStreamCommand.class);
		kryo.register(CallableStatementSetNClobCommand.class);
		kryo.register(CallableStatementSetObjectCommand.class);
		kryo.register(CallableStatementSetRowIdCommand.class);
		kryo.register(CallableStatementSetSQLXMLCommand.class);
		kryo.register(ConnectionCommitCommand.class);
		kryo.register(ConnectionPrepareCallCommand.class);
		kryo.register(ConnectionPrepareStatementCommand.class);
		kryo.register(ConnectionPrepareStatementExtendedCommand.class);
		kryo.register(ConnectionReleaseSavepointCommand.class);
		kryo.register(ConnectionRollbackWithSavepointCommand.class);
		kryo.register(ConnectionSetClientInfoCommand.class);
		kryo.register(DatabaseMetaDataGetUserNameCommand.class);
		kryo.register(DestroyCommand.class);
		kryo.register(NextRowPacketCommand.class);
		kryo.register(PingCommand.class);
		kryo.register(PreparedStatementExecuteBatchCommand.class);
		kryo.register(PreparedStatementExecuteCommand.class);
		kryo.register(PreparedStatementQueryCommand.class);
		kryo.register(PreparedStatementUpdateCommand.class);
		kryo.register(ReflectiveCommand.class);
		kryo.register(ResultSetGetMetaDataCommand.class);
		kryo.register(ResultSetProducerCommand.class);
		kryo.register(StatementCancelCommand.class);
		kryo.register(StatementExecuteBatchCommand.class);
		kryo.register(StatementExecuteCommand.class);
		kryo.register(StatementExecuteExtendedCommand.class);
		kryo.register(StatementGetGeneratedKeysCommand.class);
		kryo.register(StatementGetResultSetCommand.class);
		kryo.register(StatementQueryCommand.class);
		kryo.register(StatementUpdateCommand.class);
		kryo.register(StatementUpdateExtendedCommand.class);
		
		
		return kryo;
	}
	
	public Kryo getKryo() {
		Kryo kryo = kryoCache.poll();
		if (kryo==null){
			kryo = createKryo();
		}		
		return kryo;
	}
	
	public void releaseKryo(Kryo kryo){
		kryoCache.add(kryo);
	}	
}
