package de.simplicit.vjdbc.util;

import java.sql.BatchUpdateException;
import java.sql.DataTruncation;
import java.sql.SQLClientInfoException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

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
import de.simplicit.vjdbc.command.ConnectionCommitCommand;
import de.simplicit.vjdbc.command.ConnectionCommitCommandSerializer;
import de.simplicit.vjdbc.command.ConnectionCreateStatementCommand;
import de.simplicit.vjdbc.command.ConnectionCreateStatementCommandSerializer;
import de.simplicit.vjdbc.command.ConnectionGetAutoCommitCommand;
import de.simplicit.vjdbc.command.ConnectionGetMetaDataCommand;
import de.simplicit.vjdbc.command.ConnectionGetMetaDataCommandSerializer;
import de.simplicit.vjdbc.command.ConnectionPrepareCallCommand;
import de.simplicit.vjdbc.command.ConnectionPrepareStatementCommand;
import de.simplicit.vjdbc.command.ConnectionPrepareStatementExtendedCommand;
import de.simplicit.vjdbc.command.ConnectionReleaseSavepointCommand;
import de.simplicit.vjdbc.command.ConnectionRollbackWithSavepointCommand;
import de.simplicit.vjdbc.command.ConnectionSetAutoCommitCommand;
import de.simplicit.vjdbc.command.ConnectionSetClientInfoCommand;
import de.simplicit.vjdbc.command.DatabaseMetaDataGetDriverNameCommand;
import de.simplicit.vjdbc.command.DatabaseMetaDataGetUserNameCommand;
import de.simplicit.vjdbc.command.DatabaseMetaDataGetUserNameCommandSerializer;
import de.simplicit.vjdbc.command.DestroyCommand;
import de.simplicit.vjdbc.command.DestroyCommandSerializer;
import de.simplicit.vjdbc.command.NextRowPacketCommand;
import de.simplicit.vjdbc.command.NextRowPacketCommandSerializer;
import de.simplicit.vjdbc.command.PingCommand;
import de.simplicit.vjdbc.command.PingCommandSerializer;
import de.simplicit.vjdbc.command.PreparedStatementExecuteBatchCommand;
import de.simplicit.vjdbc.command.PreparedStatementExecuteCommand;
import de.simplicit.vjdbc.command.PreparedStatementQueryCommand;
import de.simplicit.vjdbc.command.PreparedStatementQueryCommandSerializer;
import de.simplicit.vjdbc.command.PreparedStatementUpdateCommand;
import de.simplicit.vjdbc.command.PreparedStatementUpdateCommandSerializer;
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
import de.simplicit.vjdbc.command.StatementSetFetchSizeCommand;
import de.simplicit.vjdbc.command.StatementSetFetchSizeCommandSerializer;
import de.simplicit.vjdbc.command.StatementUpdateCommand;
import de.simplicit.vjdbc.command.StatementUpdateCommandSerializer;
import de.simplicit.vjdbc.command.StatementUpdateExtendedCommand;
import de.simplicit.vjdbc.parameters.ArrayParameter;
import de.simplicit.vjdbc.parameters.ArrayParameterSerializer;
import de.simplicit.vjdbc.parameters.BigDecimalParameter;
import de.simplicit.vjdbc.parameters.BigDecimalParameterSerializer;
import de.simplicit.vjdbc.parameters.BlobParameter;
import de.simplicit.vjdbc.parameters.BlobParameterSerializer;
import de.simplicit.vjdbc.parameters.BooleanParameter;
import de.simplicit.vjdbc.parameters.BooleanParameterSerializer;
import de.simplicit.vjdbc.parameters.ByteArrayParameter;
import de.simplicit.vjdbc.parameters.ByteArrayParameterSerializer;
import de.simplicit.vjdbc.parameters.ByteParameter;
import de.simplicit.vjdbc.parameters.ByteParameterSerializer;
import de.simplicit.vjdbc.parameters.ByteStreamParameter;
import de.simplicit.vjdbc.parameters.ByteStreamParameterSerializer;
import de.simplicit.vjdbc.parameters.CharStreamParameter;
import de.simplicit.vjdbc.parameters.CharStreamParameterSerializer;
import de.simplicit.vjdbc.parameters.ClobParameter;
import de.simplicit.vjdbc.parameters.ClobParameterSerializer;
import de.simplicit.vjdbc.parameters.DateParameter;
import de.simplicit.vjdbc.parameters.DateParameterSerializer;
import de.simplicit.vjdbc.parameters.DoubleParameter;
import de.simplicit.vjdbc.parameters.DoubleParameterSerializer;
import de.simplicit.vjdbc.parameters.FloatParameter;
import de.simplicit.vjdbc.parameters.FloatParameterSerializer;
import de.simplicit.vjdbc.parameters.IntegerParameter;
import de.simplicit.vjdbc.parameters.IntegerParameterSerializer;
import de.simplicit.vjdbc.parameters.LongParameter;
import de.simplicit.vjdbc.parameters.LongParameterSerializer;
import de.simplicit.vjdbc.parameters.NStringParameter;
import de.simplicit.vjdbc.parameters.NStringParameterSerializer;
import de.simplicit.vjdbc.parameters.NullParameter;
import de.simplicit.vjdbc.parameters.NullParameterSerializer;
import de.simplicit.vjdbc.parameters.ObjectParameter;
import de.simplicit.vjdbc.parameters.ObjectParameterSerializer;
import de.simplicit.vjdbc.parameters.RefParameter;
import de.simplicit.vjdbc.parameters.RefParameterSerializer;
import de.simplicit.vjdbc.parameters.RowIdParameter;
import de.simplicit.vjdbc.parameters.RowIdParameterSerializer;
import de.simplicit.vjdbc.parameters.SQLXMLParameter;
import de.simplicit.vjdbc.parameters.SQLXMLParameterSerializer;
import de.simplicit.vjdbc.parameters.ShortParameter;
import de.simplicit.vjdbc.parameters.ShortParameterSerializer;
import de.simplicit.vjdbc.parameters.StringParameter;
import de.simplicit.vjdbc.parameters.StringParameterSerializer;
import de.simplicit.vjdbc.parameters.TimeParameter;
import de.simplicit.vjdbc.parameters.TimeParameterSerializer;
import de.simplicit.vjdbc.parameters.TimestampParameter;
import de.simplicit.vjdbc.parameters.TimestampParameterSerializer;
import de.simplicit.vjdbc.parameters.URLParameter;
import de.simplicit.vjdbc.parameters.URLParameterSerializer;
import de.simplicit.vjdbc.serial.BigDecimalColumnValues;
import de.simplicit.vjdbc.serial.BigDecimalColumnValuesSerializer;
import de.simplicit.vjdbc.serial.BooleanColumnValues;
import de.simplicit.vjdbc.serial.BooleanColumnValuesSerializer;
import de.simplicit.vjdbc.serial.ByteColumnValues;
import de.simplicit.vjdbc.serial.ByteColumnValuesSerializer;
import de.simplicit.vjdbc.serial.CallingContext;
import de.simplicit.vjdbc.serial.CallingContextSerializer;
import de.simplicit.vjdbc.serial.DoubleColumnValues;
import de.simplicit.vjdbc.serial.DoubleColumnValuesSerializer;
import de.simplicit.vjdbc.serial.FloatColumnValues;
import de.simplicit.vjdbc.serial.FloatColumnValuesSerializer;
import de.simplicit.vjdbc.serial.IntegerColumnValues;
import de.simplicit.vjdbc.serial.IntegerColumnValuesSerializer;
import de.simplicit.vjdbc.serial.LongColumnValues;
import de.simplicit.vjdbc.serial.LongColumnValuesSerializer;
import de.simplicit.vjdbc.serial.ObjectColumnValues;
import de.simplicit.vjdbc.serial.ObjectColumnValuesSerializer;
import de.simplicit.vjdbc.serial.RowPacket;
import de.simplicit.vjdbc.serial.RowPacketSerializer;
import de.simplicit.vjdbc.serial.SerialArray;
import de.simplicit.vjdbc.serial.SerialArraySerializer;
import de.simplicit.vjdbc.serial.SerialBlob;
import de.simplicit.vjdbc.serial.SerialClob;
import de.simplicit.vjdbc.serial.SerialDatabaseMetaData;
import de.simplicit.vjdbc.serial.SerialDatabaseMetaDataSerializer;
import de.simplicit.vjdbc.serial.SerialNClob;
import de.simplicit.vjdbc.serial.SerialRef;
import de.simplicit.vjdbc.serial.SerialRefSerializer;
import de.simplicit.vjdbc.serial.SerialResultSetMetaData;
import de.simplicit.vjdbc.serial.SerialRowId;
import de.simplicit.vjdbc.serial.SerialRowIdSerializer;
import de.simplicit.vjdbc.serial.ShortColumnValues;
import de.simplicit.vjdbc.serial.ShortColumnValuesSerializer;
import de.simplicit.vjdbc.serial.StreamingResultSet;
import de.simplicit.vjdbc.serial.UIDEx;
import de.simplicit.vjdbc.serial.UIDExSerializer;
import de.simplicit.vjdbc.server.command.CompositeCommand;
import de.simplicit.vjdbc.server.command.CompositeCommandSerializer;

public class KryoFactory {
	
    private final static Log logger = LogFactory.getLog(KryoFactory.class);

	private static final RowPacketSerializer ROW_PACKET_SERIALIZER = new RowPacketSerializer();
	private static final UIDExSerializer UIDEX_SERIALIZER = new UIDExSerializer();
	private static final CallingContextSerializer CALLING_CONTEXT_SERIALIZER = new CallingContextSerializer();
	private static final SerialArraySerializer SERIAL_ARRAY_SERIALIZER = new SerialArraySerializer(); 
	
	private static final SerialDatabaseMetaDataSerializer SERIAL_DATABASE_METADATA_SERIALIZER = new SerialDatabaseMetaDataSerializer();
	private static final SerialRefSerializer SERIAL_REF_SERIALIZER = new SerialRefSerializer();
	private static final SerialRowIdSerializer SERIAL_ROW_ID_SERIALIZER = new SerialRowIdSerializer();
	
	private static final ArrayParameterSerializer ARRAY_PARAMETER_SERIALIZER = new ArrayParameterSerializer();
	private static final BigDecimalParameterSerializer BIG_DECIMAL_PARAMETER_SERIALIZER = new BigDecimalParameterSerializer();
	private static final BlobParameterSerializer BLOB_PARAMETER_SERIALIZER = new BlobParameterSerializer();
	private static final BooleanParameterSerializer BOOLEAN_PARAMETER_SERIALIZER = new BooleanParameterSerializer();
	private static final ByteArrayParameterSerializer BYTE_ARRAY_PARAMETER_SERIALIZER = new ByteArrayParameterSerializer();
	private static final ByteParameterSerializer BYTE_PARAMETER_SERIALIZER = new ByteParameterSerializer();
	private static final ByteStreamParameterSerializer BYTE_STREAM_PARAMETER_SERIALIZER = new ByteStreamParameterSerializer();
	private static final CharStreamParameterSerializer CHAR_STREAM_PARAMETER_SERIALIZER = new CharStreamParameterSerializer();
	private static final ClobParameterSerializer CLOB_PARAMETER_SERIALIZER = new ClobParameterSerializer();
	private static final DateParameterSerializer DATE_PARAMETER_SERIALIZER = new DateParameterSerializer();
	private static final DoubleParameterSerializer DOUBLE_PARAMETER_SERIALIZER = new DoubleParameterSerializer();
	private static final FloatParameterSerializer FLOAT_PARAMETER_SERIALIZER = new FloatParameterSerializer();
	private static final IntegerParameterSerializer INTEGER_PARAMETER_SERIALIZER = new IntegerParameterSerializer();
	private static final LongParameterSerializer LONG_PARAMETER_SERIALIZER = new LongParameterSerializer();
	private static final NStringParameterSerializer N_STRING_PARAMETER_SERIALIZER = new NStringParameterSerializer();
	private static final NullParameterSerializer NULL_PARAMETER_SERIALIZER = new NullParameterSerializer();
	private static final ObjectParameterSerializer OBJECT_PARAMETER_SERIALIZER = new ObjectParameterSerializer();
	private static final RefParameterSerializer REF_PARAMETER_SERIALIZER = new RefParameterSerializer();
	private static final RowIdParameterSerializer ROW_ID_PARAMETER_SERIALIZER = new RowIdParameterSerializer();
	private static final SQLXMLParameterSerializer SQLXML_PARAMETER_SERIALIZER = new SQLXMLParameterSerializer();
	private static final ShortParameterSerializer SHORT_PARAMETER_SERIALIZER = new ShortParameterSerializer();
	private static final StringParameterSerializer STRING_PARAMETER_SERIALIZER = new StringParameterSerializer();
	private static final TimeParameterSerializer TIME_PARAMETER_SERIALIZER = new TimeParameterSerializer();
	private static final TimestampParameterSerializer TIMESTAMP_PARAMETER_SERIALIZER = new TimestampParameterSerializer();
	private static final URLParameterSerializer URL_PARAMETER_SERIALIZER = new URLParameterSerializer();
	
	private static final NextRowPacketCommandSerializer NEXT_ROW_PACKET_COMMAND_SERIALIZER = new NextRowPacketCommandSerializer();
	private static final DestroyCommandSerializer  DESTROY_COMMAND_SERIALIZER = new DestroyCommandSerializer();
	private static final PreparedStatementQueryCommandSerializer PREPARED_STATEMENT_QUERY_COMMAND_SERIALIZER = new PreparedStatementQueryCommandSerializer();
	private static final PingCommandSerializer PING_COMMAND_SERIALIZER = new PingCommandSerializer();
	private static final ConnectionCommitCommandSerializer CONNECTION_COMMIT_COMMAND_SERIALIZER = new ConnectionCommitCommandSerializer();
	private static final DatabaseMetaDataGetUserNameCommandSerializer DATABASE_META_DATA_GET_USER_NAME_COMMAND_SERIALIZER = new DatabaseMetaDataGetUserNameCommandSerializer();
	private static final ConnectionCreateStatementCommandSerializer CONNECTION_CREATE_STATEMENT_COMMAND_SERIALIZER = new ConnectionCreateStatementCommandSerializer();
	private static final ConnectionGetMetaDataCommandSerializer CONNECTION_GET_META_DATA_COMMAND_SERIALIZER = new ConnectionGetMetaDataCommandSerializer();
	
	private static final StatementSetFetchSizeCommandSerializer STATEMENT_SET_FETCH_SIZE_COMMAND_SERIALIZER = new StatementSetFetchSizeCommandSerializer();
	private static final PreparedStatementUpdateCommandSerializer PREPARED_STATEMENT_UPDATE_COMMAND_SERIALIZER = new PreparedStatementUpdateCommandSerializer();
	private static final StatementUpdateCommandSerializer STATEMENT_UPDATE_COMMAND_SERIALIZER = new StatementUpdateCommandSerializer();
	
	private static final CompositeCommandSerializer COMPOSITE_COMMAND_SERIALIZER = new CompositeCommandSerializer();
	
	private static final BooleanColumnValuesSerializer BOOLEAN_COLUMN_VALUES_SERIALIZER = new BooleanColumnValuesSerializer();
	private static final ByteColumnValuesSerializer BYTE_COLUMN_VALUES_SERIALIZER = new ByteColumnValuesSerializer();
	private static final ShortColumnValuesSerializer SHORT_COLUMN_VALUES_SERIALIZER = new ShortColumnValuesSerializer();
	private static final IntegerColumnValuesSerializer INTEGER_COLUMN_VALUES_SERIALIZER = new IntegerColumnValuesSerializer();
	private static final LongColumnValuesSerializer LONG_COLUMN_VALUES_SERIALIZER = new LongColumnValuesSerializer();
	private static final FloatColumnValuesSerializer FLOAT_COLUMN_VALUES_SERIALIZER = new FloatColumnValuesSerializer();
	private static final DoubleColumnValuesSerializer DOUBLE_COLUMN_VALUES_SERIALIZER = new DoubleColumnValuesSerializer();
	private static final ObjectColumnValuesSerializer OBJECT_COLUMN_VALUES_SERIALIZER = new ObjectColumnValuesSerializer();
	private static final BigDecimalColumnValuesSerializer BIG_DECIMAL_COLUMN_VALUES_SERIALIZER = new BigDecimalColumnValuesSerializer();
	
	private final ConcurrentLinkedQueue<Kryo> kryoCache = new ConcurrentLinkedQueue<Kryo>();
	
	private ConcurrentMap<String, AtomicInteger> instanceCount = new ConcurrentHashMap<String, AtomicInteger>();
	/** 
	 * Utility class for gathering statistic number of instantiations via reflection
	 */
	class KryoSerializableCountingSerializer extends Serializer<KryoSerializable> {
		public void write (Kryo kryo, Output output, KryoSerializable object) {
			object.write(kryo, output);
		}

		public KryoSerializable read (Kryo kryo, Input input, Class<KryoSerializable> type) {
			count(type.getName());
			KryoSerializable object = kryo.newInstance(type);
			kryo.reference(object);
			object.read(kryo, input);
			return object;
		}
		
		private void count(String type){
			AtomicInteger i = new AtomicInteger(1);
			AtomicInteger v = instanceCount.putIfAbsent(type, i);
			if (v!=null){
				v.incrementAndGet();
			}			
		}
		
	}
	
	private final KryoSerializableCountingSerializer KRYO_SERIALIZABLE_COUNTING_SERIALIZER = new KryoSerializableCountingSerializer();
	
	public void dumpInstanceCount(){
		int size = instanceCount.size();
		if (size>0){			
			logger.debug("==== Instance count dump ====");
			ArrayList<Map.Entry<String,AtomicInteger>> list = new ArrayList<Map.Entry<String, AtomicInteger>>(instanceCount.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String,AtomicInteger>>() {
				@Override
				public int compare(Entry<String, AtomicInteger> o1, Entry<String, AtomicInteger> o2) {
					return Integer.valueOf(o1.getValue().get()).compareTo(Integer.valueOf(o2.getValue().get()));
				}
			});
			
			for (Map.Entry<String,AtomicInteger> me: list){
				logger.debug("class: "+me.getKey()+": "+me.getValue().get());
			}
		}
	}
	

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

		// java.sql exceptions
		kryo.register(BatchUpdateException.class, new JavaSerializer());
		kryo.register(DataTruncation.class, new JavaSerializer());
		kryo.register(SQLClientInfoException.class, new JavaSerializer());
		kryo.register(SQLDataException.class, new JavaSerializer());		
		kryo.register(SQLException.class, new JavaSerializer());
		kryo.register(SQLFeatureNotSupportedException.class, new JavaSerializer());
		kryo.register(SQLIntegrityConstraintViolationException.class, new JavaSerializer());
		kryo.register(SQLInvalidAuthorizationSpecException.class, new JavaSerializer());
		kryo.register(SQLNonTransientConnectionException.class, new JavaSerializer());
		kryo.register(SQLNonTransientException.class, new JavaSerializer());
		kryo.register(SQLRecoverableException.class, new JavaSerializer());
		kryo.register(SQLSyntaxErrorException.class, new JavaSerializer());
		kryo.register(SQLTimeoutException.class, new JavaSerializer());
		kryo.register(SQLTransactionRollbackException.class, new JavaSerializer());
		kryo.register(SQLTransientConnectionException.class, new JavaSerializer());
		kryo.register(SQLTransientException.class, new JavaSerializer());
		kryo.register(SQLWarning.class, new JavaSerializer());
		
		kryo.register(VJdbcException.class, new JavaSerializer());
		kryo.register(UIDEx.class, UIDEX_SERIALIZER);
		kryo.register(CallingContext.class, CALLING_CONTEXT_SERIALIZER);
		
		kryo.register(BooleanColumnValues.class, BOOLEAN_COLUMN_VALUES_SERIALIZER);
		kryo.register(ByteColumnValues.class, BYTE_COLUMN_VALUES_SERIALIZER);
		kryo.register(ShortColumnValues.class, SHORT_COLUMN_VALUES_SERIALIZER);
		kryo.register(IntegerColumnValues.class, INTEGER_COLUMN_VALUES_SERIALIZER);
		kryo.register(LongColumnValues.class, LONG_COLUMN_VALUES_SERIALIZER);
		kryo.register(FloatColumnValues.class, FLOAT_COLUMN_VALUES_SERIALIZER);
		kryo.register(DoubleColumnValues.class, DOUBLE_COLUMN_VALUES_SERIALIZER);
		kryo.register(ObjectColumnValues.class, OBJECT_COLUMN_VALUES_SERIALIZER);
		kryo.register(BigDecimalColumnValues.class, BIG_DECIMAL_COLUMN_VALUES_SERIALIZER);
		
		kryo.register(RowPacket.class, ROW_PACKET_SERIALIZER);
		kryo.register(SerialArray.class, SERIAL_ARRAY_SERIALIZER);
		kryo.register(SerialDatabaseMetaData.class, SERIAL_DATABASE_METADATA_SERIALIZER);
		kryo.register(SerialBlob.class);
		kryo.register(SerialClob.class);
		kryo.register(SerialNClob.class);
		kryo.register(SerialRef.class, SERIAL_REF_SERIALIZER);
		kryo.register(SerialResultSetMetaData.class);
		kryo.register(SerialRowId.class, SERIAL_ROW_ID_SERIALIZER);
		kryo.register(StreamingResultSet.class);
		
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
		kryo.register(ConnectionCommitCommand.class, CONNECTION_COMMIT_COMMAND_SERIALIZER);
		kryo.register(ConnectionPrepareCallCommand.class);
		kryo.register(ConnectionPrepareStatementCommand.class);
		kryo.register(ConnectionPrepareStatementExtendedCommand.class);
		kryo.register(ConnectionReleaseSavepointCommand.class);
		kryo.register(ConnectionRollbackWithSavepointCommand.class);
		kryo.register(ConnectionSetClientInfoCommand.class);
		kryo.register(DatabaseMetaDataGetUserNameCommand.class, DATABASE_META_DATA_GET_USER_NAME_COMMAND_SERIALIZER);
		kryo.register(DestroyCommand.class, DESTROY_COMMAND_SERIALIZER);
		kryo.register(NextRowPacketCommand.class, NEXT_ROW_PACKET_COMMAND_SERIALIZER);
		kryo.register(PingCommand.class, PING_COMMAND_SERIALIZER);
		kryo.register(PreparedStatementExecuteBatchCommand.class);
		kryo.register(PreparedStatementExecuteCommand.class);
		kryo.register(PreparedStatementQueryCommand.class, PREPARED_STATEMENT_QUERY_COMMAND_SERIALIZER);
		kryo.register(PreparedStatementUpdateCommand.class, PREPARED_STATEMENT_UPDATE_COMMAND_SERIALIZER);
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
		kryo.register(StatementUpdateCommand.class, STATEMENT_UPDATE_COMMAND_SERIALIZER);
		kryo.register(StatementUpdateExtendedCommand.class);
		kryo.register(ConnectionCreateStatementCommand.class, CONNECTION_CREATE_STATEMENT_COMMAND_SERIALIZER);
		kryo.register(ConnectionGetAutoCommitCommand.class);
		kryo.register(ConnectionGetMetaDataCommand.class, CONNECTION_GET_META_DATA_COMMAND_SERIALIZER);
		kryo.register(ConnectionSetAutoCommitCommand.class);
		kryo.register(DatabaseMetaDataGetDriverNameCommand.class);
		kryo.register(StatementSetFetchSizeCommand.class, STATEMENT_SET_FETCH_SIZE_COMMAND_SERIALIZER);
		kryo.register(CompositeCommand.class, COMPOSITE_COMMAND_SERIALIZER);
		
		// Parameters
		kryo.register(ArrayParameter.class, ARRAY_PARAMETER_SERIALIZER);
		kryo.register(BigDecimalParameter.class, BIG_DECIMAL_PARAMETER_SERIALIZER);
		kryo.register(BlobParameter.class, BLOB_PARAMETER_SERIALIZER);
		kryo.register(BooleanParameter.class, BOOLEAN_PARAMETER_SERIALIZER);
		kryo.register(ByteArrayParameter.class, BYTE_ARRAY_PARAMETER_SERIALIZER);
		kryo.register(ByteParameter.class, BYTE_PARAMETER_SERIALIZER);
		kryo.register(ByteStreamParameter.class, BYTE_STREAM_PARAMETER_SERIALIZER);
		kryo.register(CharStreamParameter.class, CHAR_STREAM_PARAMETER_SERIALIZER);
		kryo.register(ClobParameter.class, CLOB_PARAMETER_SERIALIZER);
		kryo.register(DateParameter.class, DATE_PARAMETER_SERIALIZER);
		kryo.register(DoubleParameter.class, DOUBLE_PARAMETER_SERIALIZER);
		kryo.register(FloatParameter.class, FLOAT_PARAMETER_SERIALIZER);
		kryo.register(IntegerParameter.class, INTEGER_PARAMETER_SERIALIZER);
		kryo.register(LongParameter.class, LONG_PARAMETER_SERIALIZER);
		kryo.register(NStringParameter.class, N_STRING_PARAMETER_SERIALIZER);
		kryo.register(NullParameter.class, NULL_PARAMETER_SERIALIZER);
		kryo.register(ObjectParameter.class, OBJECT_PARAMETER_SERIALIZER);
		kryo.register(RefParameter.class, REF_PARAMETER_SERIALIZER);
		kryo.register(RowIdParameter.class, ROW_ID_PARAMETER_SERIALIZER);
		kryo.register(SQLXMLParameter.class, SQLXML_PARAMETER_SERIALIZER);
		kryo.register(ShortParameter.class, SHORT_PARAMETER_SERIALIZER);
		kryo.register(StringParameter.class, STRING_PARAMETER_SERIALIZER);
		kryo.register(TimeParameter.class, TIME_PARAMETER_SERIALIZER);
		kryo.register(TimestampParameter.class, TIMESTAMP_PARAMETER_SERIALIZER);
		kryo.register(URLParameter.class, URL_PARAMETER_SERIALIZER);
		
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
