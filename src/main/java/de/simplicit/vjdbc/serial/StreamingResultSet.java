// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import de.simplicit.vjdbc.command.*;
import de.simplicit.vjdbc.util.JavaVersionInfo;
import de.simplicit.vjdbc.util.SQLExceptionHelper;
import de.simplicit.vjdbc.VirtualStatement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class StreamingResultSet implements ResultSet, Externalizable,KryoSerializable {
    static final long serialVersionUID = 8291019975153433161L;

    private static Log _logger = LogFactory.getLog(StreamingResultSet.class);

    private String[] _columnNames;
    private String[] _columnLabels;

    private transient List<RowPacket> _pages = null;

    private int _rowPacketSize;
    private boolean _forwardOnly;
    private String _charset;
    private boolean _lastPartReached = true;
    private UIDEx _remainingResultSet = null;
    private SerialResultSetMetaData _metaData = null;

    private transient DecoratedCommandSink _commandSink = null;
    /** index inside current page in _columnValues */
    private transient int _cursor = -1;

    /** current page (aka {@link RowPacket})
     * _page.getIndex()*_rowPacketSize +_cursor = current row number */
    private transient RowPacket _page = null;
    private transient int _lastReadColumn = -1;
    /** column values of current page */
    private transient ColumnValues[] _columnValues;
    private transient int _fetchDirection;
    private transient boolean _prefetchMetaData;
    private transient Statement _statement;

    protected void finalize() throws Throwable {
        super.finalize();
        if(_remainingResultSet != null) {
            close();
        }
    }

    public StreamingResultSet() {
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_columnNames);
        out.writeObject(_columnLabels);
        out.writeObject(_page);
        out.writeInt(_rowPacketSize);
        out.writeBoolean(_forwardOnly);
        out.writeUTF(_charset);
        out.writeBoolean(_lastPartReached);
        out.writeObject(_remainingResultSet);
        out.writeObject(_metaData);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _columnNames = (String[])in.readObject();
        _columnLabels = (String[])in.readObject();
        _page = (RowPacket)in.readObject();
        _rowPacketSize = in.readInt();
        _forwardOnly = in.readBoolean();
        _charset = in.readUTF();
        _lastPartReached = in.readBoolean();
        _remainingResultSet = (UIDEx)in.readObject();
        _metaData = (SerialResultSetMetaData)in.readObject();

        // initialization
        _columnValues = _page.getFlattenedColumnsValues();
        if (!_forwardOnly){
        	_pages = new ArrayList<RowPacket>();
        	_pages.add(_page);
        }
    }

    public StreamingResultSet(int rowPacketSize, boolean forwardOnly, boolean prefetchMetaData, String charset) {
        _rowPacketSize = rowPacketSize;
        _forwardOnly = forwardOnly;
        _prefetchMetaData = prefetchMetaData;
        _charset = charset;
    }

    public void setStatement(Statement stmt) {
        _statement = stmt;
    }

    public void setCommandSink(DecoratedCommandSink sink) {
        _commandSink = sink;
    }

    public void setRemainingResultSetUID(UIDEx reg) {
        _remainingResultSet = reg;
    }

    public boolean populate(ResultSet rs, ResultSetMetaData metaData) throws SQLException {

        // Fetch the meta data immediately if required. Succeeding getMetaData() calls
        // on the ResultSet won't require an additional remote call
        if(_prefetchMetaData) {
            _logger.debug("Fetching MetaData of ResultSet");
            _metaData = new SerialResultSetMetaData(metaData);
            // use already retrieved arrays
            _columnNames = _metaData.getColumnNames();
            _columnLabels = _metaData.getColumnLabels();
        } else {
	        int columnCount = metaData.getColumnCount();
	        _columnNames = new String[columnCount];
	        _columnLabels = new String[columnCount];
	
	        for(int i = 1; i <= columnCount; i++) {
	            _columnNames[i-1] = metaData.getColumnName(i);
	            _columnLabels[i-1] = metaData.getColumnLabel(i);
	        }
        }
        // Create first ResultSet-Part
         _page = new RowPacket(_rowPacketSize /*, _forwardOnly */, 0);
        // Populate it         
         _page.populate(rs, metaData);

        _lastPartReached = _page.isLastPart();

        return _lastPartReached;
    }

    public boolean next() throws SQLException {
//  TODO      if (_page==null){
//        	throw new SQLException("ResultSet is closed");
//        }
    	_cursor++;
        if (_cursor < _page.getRowCount()){
        	// we are still within the same page
        	_lastReadColumn = -1;
        	return true;
        }
        int pageIndex = _page.getIndex()+1;
        if (_pages!=null && pageIndex <_pages.size()){
        	_page = _pages.get(pageIndex);
        	_columnValues = _page.getFlattenedColumnsValues();
        	_lastReadColumn = -1;
        	_cursor = 0;
        } else  if (requestNextRowPacket()) {
        	_cursor = 0;
        }
        return _cursor < _page.getRowCount();
    }

    public void close() throws SQLException {
        _cursor = -1;
        _page = null;
        _lastReadColumn = -1;
        if(_remainingResultSet != null) {
            // The server-side created StreamingResultSet is garbage-collected after it was send over the wire. Thus
            // we have to check here if it is such a server object because in this case we don't have to try the remote
            // call which indeed causes a NPE.
            if(_commandSink != null) {
                _commandSink.process(_remainingResultSet, new DestroyCommand(_remainingResultSet, JdbcInterfaceType.RESULTSETHOLDER));
            }
            _remainingResultSet = null;
        }
//        if (_statement.isCloseOnCompletion()) {
//            _statement.close();
//        }
    }

    public boolean wasNull() throws SQLException {
        return (_lastReadColumn>=0) && _columnValues[_lastReadColumn].isNull(_cursor);
    }

    public String getString(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getString(_cursor);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getBoolean(_cursor);
    }

    public byte getByte(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getByte(_cursor);
    }

    public short getShort(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getShort(_cursor);
    }

    public int getInt(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getInt(_cursor);
    }

    public long getLong(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getLong(_cursor);
    }

    public float getFloat(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getFloat(_cursor);
    }

    public double getDouble(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getDouble(_cursor);        
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        BigDecimal result = _columnValues[columnIndex].getBigDecimal(_cursor);
        result.setScale(scale);
		return result;
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (byte[]) _columnValues[columnIndex].getObject(_cursor);         
    }

    public Date getDate(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
         return _columnValues[columnIndex].getDate(_cursor); 
    }

    public Time getTime(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getTime(_cursor); 
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getTimestamp(_cursor);         
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("getAsciiStream");
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("getUnicodeStream");
    }

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		columnIndex--;
		_lastReadColumn = columnIndex;

		Object obj = _columnValues[columnIndex].getObject(_cursor);
		if (obj == null) {
			return null;
		}

		byte[] bytes;

		if (obj instanceof byte[]) {
			bytes = (byte[]) obj;
		} else if (obj instanceof String) {
			try {
				bytes = ((String) obj).getBytes(_charset);
			} catch (UnsupportedEncodingException e) {
				throw SQLExceptionHelper.wrap(e);
			}
		} else {
			String msg = "StreamingResultSet.getBinaryStream(): Can't convert object of type '" + obj.getClass()
					+ "' to InputStream";
			throw new SQLException(msg);
		}

		return new ByteArrayInputStream(bytes);
	}

    public String getString(String columnName) throws SQLException {
        return getString(getIndexForName(columnName));
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(getIndexForName(columnName));
    }

    public byte getByte(String columnName) throws SQLException {
        return getByte(getIndexForName(columnName));
    }

    public short getShort(String columnName) throws SQLException {
        return getShort(getIndexForName(columnName));
    }

    public int getInt(String columnName) throws SQLException {
        return getInt(getIndexForName(columnName));
    }

    public long getLong(String columnName) throws SQLException {
        return getLong(getIndexForName(columnName));
    }

    public float getFloat(String columnName) throws SQLException {
        return getFloat(getIndexForName(columnName));
    }

    public double getDouble(String columnName) throws SQLException {
        return getDouble(getIndexForName(columnName));
    }

    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return getBigDecimal(getIndexForName(columnName), scale);
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(getIndexForName(columnName));
    }

    public Date getDate(String columnName) throws SQLException {
        return getDate(getIndexForName(columnName));
    }

    public Time getTime(String columnName) throws SQLException {
        return getTime(getIndexForName(columnName));
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(getIndexForName(columnName));
    }

    public InputStream getAsciiStream(String columnName) throws SQLException {
        throw new UnsupportedOperationException("getAsciiStream");
    }

    public InputStream getUnicodeStream(String columnName) throws SQLException {
        throw new UnsupportedOperationException("getUnicodeStream");
    }

    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(getIndexForName(columnName));
    }

    public SQLWarning getWarnings() throws SQLException {
        if(_cursor < 0) {
            throw new SQLException("ResultSet already closed");
        } else {
            return null;
        }
    }

    public void clearWarnings() throws SQLException {
    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException("getCursorName");
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        if(_metaData == null) {
        	_metaData = (SerialResultSetMetaData)_commandSink.process(_remainingResultSet, new ResultSetGetMetaDataCommand());
        }

        return _metaData;
    }

    public Object getObject(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getObject(_cursor);
    }

    public Object getObject(String columnName) throws SQLException {
        return getObject(getIndexForName(columnName));
    }

    public int findColumn(String columnName) throws SQLException {
        return getIndexForName(columnName);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        Object value = _columnValues[columnIndex].getObject(_cursor);
        if (value==null){
        	return null;
        }
		return new StringReader((String)value);
    }

    public Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream(getIndexForName(columnName));
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getBigDecimal(_cursor);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(getIndexForName(columnName));
    }

    public boolean isBeforeFirst() throws SQLException {
        return _cursor < 0;
    }

    public boolean isAfterLast() throws SQLException {
        if (_page==null) { 
        	throw new SQLException("ResultSet is closed");
        }

        return _lastPartReached && (_forwardOnly || _page.getIndex()==_pages.size()-1) && (_cursor>=_page.getRowCount());
    }

    public boolean isFirst() throws SQLException {
        return _cursor == 0 && _page.getIndex()==0;
    }

    public boolean isLast() throws SQLException {
        return _lastPartReached && (_forwardOnly || _page.getIndex()==_pages.size()-1) && (_cursor==_page.getRowCount()-1);
    }

    public void beforeFirst() throws SQLException {
    	if (_forwardOnly){
    		throw new SQLException("beforeFirst() is not possible on Forward-Only-ResultSet");
    	}
    	_cursor = -1;
        _page = _pages.get(0);// first packet is always present 
        _columnValues = _page.getFlattenedColumnsValues();
        _lastReadColumn = -1;
    }

    public void afterLast() throws SQLException {
        // Request all remaining Row-Packets
        while(requestNextRowPacket()) ;

        _cursor = _page.getRowCount();
    }

    public boolean first() throws SQLException {
		if (_forwardOnly) {
			throw new SQLException("previous() is not possible on Forward-Only-ResultSet");
		}

		_page = _pages.get(0);
		_columnValues = _page.getFlattenedColumnsValues();
		_lastReadColumn = -1;
		_cursor = 0;
		return _cursor < _page.getRowCount();
    }

    public boolean last() throws SQLException {
        // Request all remaining Row-Packets
        while(requestNextRowPacket()) ;

        _cursor = _page.getRowCount() - 1;
        return _cursor>=0;
    }

    public int getRow() throws SQLException {
        return _page.getIndex() * _rowPacketSize + _cursor + 1;
    }

    public boolean absolute(int row) throws SQLException {
        return setCursor(row - 1);
    }

    public boolean relative(int step) throws SQLException {
        return setCursor(_page.getIndex() * _rowPacketSize + _cursor + step);
    }

    public boolean previous() throws SQLException {
        if(_forwardOnly) {
            throw new SQLException("previous() not possible on Forward-Only-ResultSet");
        }
    	_cursor--;
    	if (_cursor<0 && _page.getIndex()>0){
    		// switch to previous page 
    		_page = _pages.get(_page.getIndex()-1);
    		_columnValues = _page.getFlattenedColumnsValues();
    		_lastReadColumn = -1;
    		_cursor = _page.getRowCount() - 1;
    	}
    	return _cursor>=0;
    }

    public void setFetchDirection(int direction) throws SQLException {
        _fetchDirection = direction;
    }

    public int getFetchDirection() throws SQLException {
        return _fetchDirection;
    }

    public void setFetchSize(int rows) throws SQLException {
    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getType() throws SQLException {
        return _forwardOnly ? ResultSet.TYPE_FORWARD_ONLY : ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    public boolean rowUpdated() throws SQLException {
        return false;
    }

    public boolean rowInserted() throws SQLException {
        return false;
    }

    public boolean rowDeleted() throws SQLException {
        return false;
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("updateNull");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException("updateBoolean");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException("updateByte");
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException("updateShort");
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException("updateInt");
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException("updateLong");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException("updateFloat");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException("updateDouble");
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("updateBigDecimal");
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException("updateString");
    }

    public void updateBytes(int columnIndex, byte x[]) throws SQLException {
        throw new UnsupportedOperationException("updateBytes");
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException("updateDate");
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException("updateTime");
    }

    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException {
        throw new UnsupportedOperationException("updateTimestamp");
    }

    public void updateAsciiStream(int columnIndex,
                                  InputStream x,
                                  int length) throws SQLException {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    public void updateBinaryStream(int columnIndex,
                                   InputStream x,
                                   int length) throws SQLException {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    public void updateCharacterStream(int columnIndex,
                                      Reader x,
                                      int length) throws SQLException {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException {
        throw new UnsupportedOperationException("updateObject");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException("updateObject");
    }

    public void updateNull(String columnName) throws SQLException {
        throw new UnsupportedOperationException("updateNull");
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        throw new UnsupportedOperationException("updateBoolean");
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        throw new UnsupportedOperationException("updateByte");
    }

    public void updateShort(String columnName, short x) throws SQLException {
        throw new UnsupportedOperationException("updateShort");
    }

    public void updateInt(String columnName, int x) throws SQLException {
        throw new UnsupportedOperationException("updateInt");
    }

    public void updateLong(String columnName, long x) throws SQLException {
        throw new UnsupportedOperationException("updateLong");
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        throw new UnsupportedOperationException("updateFloat");
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        throw new UnsupportedOperationException("updateDouble");
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException("updateBigDecimal");
    }

    public void updateString(String columnName, String x) throws SQLException {
        throw new UnsupportedOperationException("updateString");
    }

    public void updateBytes(String columnName, byte x[]) throws SQLException {
        throw new UnsupportedOperationException("updateBytes");
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        throw new UnsupportedOperationException("updateDate");
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        throw new UnsupportedOperationException("updateTime");
    }

    public void updateTimestamp(String columnName, Timestamp x)
            throws SQLException {
        throw new UnsupportedOperationException("updateTimestamp");
    }

    public void updateAsciiStream(String columnName,
                                  InputStream x,
                                  int length) throws SQLException {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    public void updateBinaryStream(String columnName,
                                   InputStream x,
                                   int length) throws SQLException {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    public void updateCharacterStream(String columnName,
                                      Reader reader,
                                      int length) throws SQLException {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    public void updateObject(String columnName, Object x, int scale)
            throws SQLException {
        throw new UnsupportedOperationException("updateObject");
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        throw new UnsupportedOperationException("updateObject");
    }

    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException("insertRow");
    }

    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException("updateRow");
    }

    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException("deleteRow");
    }

    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException("refreshRow");
    }

    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException("cancelRowUpdates");
    }

    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException("moveToInsertRow");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException("moveToCurrentRow");
    }

    public Statement getStatement() throws SQLException {
        return _statement;
    }

    public Object getObject(int i, Map map) throws SQLException {
        throw new UnsupportedOperationException("getObject");
    }

    public Ref getRef(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (Ref) _columnValues[columnIndex].getObject(_cursor);
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (Blob) _columnValues[columnIndex].getObject(_cursor);
    }

    public Clob getClob(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (Clob) _columnValues[columnIndex].getObject(_cursor);        
    }

    public Array getArray(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (Array) _columnValues[columnIndex].getObject(_cursor);        
    }

    public Object getObject(String colName, Map map) throws SQLException {
        throw new UnsupportedOperationException("getObject");
    }

    public <T> T getObject(String columnName, Class<T> clazz) {
        throw new UnsupportedOperationException("getObject(String, Class)");
    }

    public <T> T getObject(int columnIndex, Class<T> clazz) {
        throw new UnsupportedOperationException("getObject(int, Class)");
    }

    public Ref getRef(String colName) throws SQLException {
        return getRef(getIndexForName(colName));
    }

    public Blob getBlob(String colName) throws SQLException {
        return getBlob(getIndexForName(colName));
    }

    public Clob getClob(String colName) throws SQLException {
        return getClob(getIndexForName(colName));
    }

    public Array getArray(String colName) throws SQLException {
        return getArray(getIndexForName(colName));
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        columnIndex--;
        Date date = getDate(columnIndex);
        if (date!=null){
        	cal.setTime(date);
            return (Date)cal.getTime();
        }
        return null;
    }

    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate(getIndexForName(columnName), cal);
    }

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		columnIndex--;
		Time time = (Time) getTime(columnIndex);
		if (time != null) {
			cal.setTime(time);
			return (Time) cal.getTime();
		}
		return null;
	}

    public Time getTime(String columnName, Calendar cal) throws SQLException {
        return getTime(getIndexForName(columnName), cal);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        Timestamp timestamp = getTimestamp(columnIndex);
        if(timestamp != null) {
            cal.setTime(timestamp);
            return (Timestamp)cal.getTime();
        }
        else {
            return null;
        }
    }

    public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
        return getTimestamp(getIndexForName(columnName), cal);
    }

    public URL getURL(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (URL)_columnValues[columnIndex].getObject(_cursor);
    }

    public URL getURL(String columnName) throws SQLException {
        return getURL(getIndexForName(columnName));
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException("updateRef");
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        throw new UnsupportedOperationException("updateRef");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException("updateBlob");
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        throw new UnsupportedOperationException("updateBlob");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException("updateClob");
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        throw new UnsupportedOperationException("updateClob");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException("updateArray");
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        throw new UnsupportedOperationException("updateArray");
    }

    private int getIndexForName(String name) throws SQLException {
        int result = -1;
        // first search in the columns names (hit is very likely)
        for(int i = 0; i < _columnNames.length; ++i) {
            if(_columnNames[i].equalsIgnoreCase(name)) {
                result = i;
                break;
            }
        }
        // not found ? then search in the labels
        if(result < 0) {
                for(int i = 0; i < _columnLabels.length; ++i) {
                    if(_columnLabels[i].equalsIgnoreCase(name)) {
                        result = i;
                        break;
                    }
                }
        }
        if(result < 0) {
            throw new SQLException("Unknown column " + name);
        }
        else {
            _lastReadColumn = result;
        }

        return result + 1;
    }

    /**
     * retrieve next page and make it current
     * if retrieved page contains zero rows, 
     * current page is not changed
     * does not change the cursor
     * @return
     * @throws SQLException
     */
    private final boolean requestNextRowPacket() throws SQLException {
		if (!_lastPartReached) {
			RowPacket rsp = (RowPacket) _commandSink.process(_remainingResultSet, NextRowPacketCommand.INSTANCE);
			if (rsp.isLastPart()) {
				_lastPartReached = true;
			}
			if (rsp.getRowCount()>0){
				if (!_forwardOnly) {
					_pages.add(rsp);
				}
				_page = rsp;
				_columnValues = rsp.getFlattenedColumnsValues();
				_lastReadColumn = -1;
				return true;
			}
		}
		return false;
    }

    private final boolean setCursor(int row) throws SQLException {
        if (row<0){
        	return false;
        }
    	int pageIndex = row / _rowPacketSize;
    	if (_forwardOnly && pageIndex < _page.getIndex()){
    		throw new SQLException("Moving backward is not possible on Forward-Only-ResultSet");
    	}
    	if (_pages!=null && pageIndex<_pages.size()){
    		_page = _pages.get(pageIndex);
    		_columnValues = _page.getFlattenedColumnsValues();
    		_cursor = row - (pageIndex * _rowPacketSize);
    		_lastReadColumn = -1;
    		return true;
    	} else {
    		while(requestNextRowPacket() && _page.getIndex()!=pageIndex);
    		if (pageIndex==_page.getIndex()){
    			_cursor = row - (pageIndex * _rowPacketSize);
    			return true;
    		} else {
    			_cursor = _page.getRowCount();
    		}
    	}
    	return false;
    }

    private Date getCleanDate(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    private Time getCleanTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MILLISECOND, 0);
        return new Time(cal.getTimeInMillis());
    }

    /* start JDBC4 support */
    public RowId getRowId(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (RowId) _columnValues[columnIndex].getObject(_cursor);
    }

    public RowId getRowId(String columnName) throws SQLException {
    	return getRowId(getIndexForName(columnName));
    }

    public void setRowId(String parameterName, RowId x) throws SQLException {
        throw new UnsupportedOperationException("setRowId");
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException("updateRowId");
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException("updateRowId");
    }

    public int getHoldability() throws SQLException {
        return _commandSink.processWithIntResult(_remainingResultSet, CommandPool.getReflectiveCommand(JdbcInterfaceType.RESULTSETHOLDER, "getHoldability"));
    }

    public boolean isClosed() throws SQLException {
        return (_cursor < 0);
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException("updateNString");
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException("updateNString");
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("updateNClob");
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException("updateNClob");
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (NClob) _columnValues[columnIndex].getObject(_cursor);        
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(getIndexForName(columnLabel));
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return (SQLXML) _columnValues[columnIndex].getObject(_cursor);          
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(getIndexForName(columnLabel));
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("updateSQLXML");
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException("updateSQLXML");
    }

    public String getNString(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        return _columnValues[columnIndex].getString(_cursor);        
    }

    public String getNString(String columnLabel) throws SQLException {
        return getNString(getIndexForName(columnLabel));
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        columnIndex--;
        _lastReadColumn = columnIndex;
        String s = _columnValues[columnIndex].getString(_cursor);
		if (s!=null){
			return new StringReader(s);
		}
        return null;
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(getIndexForName(columnLabel));
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    public void updateNCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException("updateBlob");
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("updateClob");
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("updateClob");
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("updateNClob");
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException("updateNClob");
    }

    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateNCharacterStream");
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    public void updateCharacterStream(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateCharacterStream");
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("updateAsciiStream");
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException("updateBinaryStream");
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("updateBlob");
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException("updateBlob");
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateClob");
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateClob");
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateNClob");
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException("updateNClob");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(StreamingResultSet.class);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)this;
    }
    /* end JDBC4 support */

	@Override
	public void write(Kryo kryo, Output output) {
		kryo.writeObjectOrNull(output, _metaData, SerialResultSetMetaData.class);		
		if (_metaData==null){
			// write partial metadata only if full metadata is absent
	    	kryo.writeObjectOrNull(output, _columnNames, String[].class);
	    	kryo.writeObjectOrNull(output, _columnLabels, String[].class);
	    }
	    
	    kryo.writeObjectOrNull(output, _page, RowPacket.class);
	    output.writeInt(_rowPacketSize);
	    output.writeBoolean(_forwardOnly);
	    kryo.writeObjectOrNull(output, _charset, String.class);
	    kryo.writeObjectOrNull(output, _remainingResultSet, UIDEx.class);
	}

	@Override
	public void read(Kryo kryo, Input input) {
		_metaData = kryo.readObjectOrNull(input, SerialResultSetMetaData.class);		
		if (_metaData==null){
	    	_columnNames = kryo.readObjectOrNull(input, String[].class);
	    	_columnLabels = kryo.readObjectOrNull(input, String[].class);
	    } else {
	    	_columnNames = _metaData.getColumnNames();
	    	_columnLabels = _metaData.getColumnLabels();
	    }
	    
		_page = kryo.readObjectOrNull(input, RowPacket.class);
	    _lastPartReached = _page.isLastPart();
	    
	    _rowPacketSize = input.readInt();
	    _forwardOnly = input.readBoolean();
	    _charset = kryo.readObjectOrNull(input, String.class);
	    
	    _remainingResultSet = kryo.readObjectOrNull(input, UIDEx.class);
	    
	    // initialization
	    if (!_forwardOnly){
	    	_pages = new ArrayList<RowPacket>();
	    	_pages.add(_page);
	    }
        _columnValues = _page.getFlattenedColumnsValues();
	}
}
