// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.simplicit.vjdbc.util.JavaVersionInfo;

/**
 * A RowPacket contains the data of a part (or a whole) JDBC-ResultSet.
 */
public class RowPacket implements Externalizable {
    static final int ORACLE_ROW_ID = -8;
    private static final int DEFAULT_ARRAY_SIZE = 100;
    static final long serialVersionUID = 6366194574502000718L;

    private static Log _logger = LogFactory.getLog(RowPacket.class);

    private int _rowCount = 0;
    private boolean _lastPart = false;
    private int _index;
    private ColumnValues[] _flattenedColumnsValues = null;
    
    // Transient attributes
    private transient int[] _columnTypes = null;
    private transient int _maxrows = 0;

    public RowPacket() {
    	
    }

    public RowPacket(int packetsize, int index) {    	
    	_maxrows = packetsize<=0?DEFAULT_ARRAY_SIZE:packetsize;
        _index = index;
    }

    
    RowPacket(int index, boolean lastPart, int rowCount, ColumnValues[] flattenedColumnsValues){
    	this._index = index;
    	this._lastPart = lastPart;
    	this._rowCount = rowCount;
    	this._flattenedColumnsValues = flattenedColumnsValues;
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
    	out.writeInt(_index);
    	out.writeBoolean(_lastPart);
        out.writeInt(_rowCount);
        if(_rowCount > 0) {
            out.writeObject(_flattenedColumnsValues);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    	_index = in.readInt();
    	_lastPart = in.readBoolean();
        _rowCount = in.readInt();
        if (_rowCount>0){
        	_flattenedColumnsValues = (ColumnValues[]) in.readObject();
        }
    }

    public boolean isLastPart() {
    	return _lastPart;
    }

    public boolean populate(ResultSet rs, ResultSetMetaData metaData) throws SQLException {

        int columnCount = metaData.getColumnCount();
        _rowCount = 0;
        prepareFlattenedColumns(metaData, columnCount);

        while (_rowCount<_maxrows && rs.next()) {

            for(int i = 1; i <= columnCount; i++) {
                boolean foundMatch = true;

                int internalIndex = i - 1;

                switch (_columnTypes[internalIndex]) {
                case Types.NULL:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, null);
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, rs.getString(i));
                    break;

                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, rs.getNString(i));
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, rs.getBigDecimal(i));
                    break;

                case Types.BIT:
                    _flattenedColumnsValues[internalIndex].setBoolean(_rowCount, rs.getBoolean(i));
                    break;

                case Types.TINYINT:
                    _flattenedColumnsValues[internalIndex].setByte(_rowCount, rs.getByte(i));
                    break;

                case Types.SMALLINT:
                    _flattenedColumnsValues[internalIndex].setShort(_rowCount, rs.getShort(i));
                    break;

                case Types.INTEGER:
                    _flattenedColumnsValues[internalIndex].setInt(_rowCount, rs.getInt(i));
                    break;

                case Types.BIGINT:
                    _flattenedColumnsValues[internalIndex].setLong(_rowCount, rs.getLong(i));
                    break;

                case Types.REAL:
                    _flattenedColumnsValues[internalIndex].setFloat(_rowCount, rs.getFloat(i));
                    break;

                case Types.FLOAT:
                case Types.DOUBLE:
                    _flattenedColumnsValues[internalIndex].setDouble(_rowCount, rs.getDouble(i));
                    break;

                case Types.DATE:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, rs.getDate(i));
                    break;

                case Types.TIME:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, rs.getTime(i));
                    break;

                case Types.TIMESTAMP:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, rs.getTimestamp(i));
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, rs.getBytes(i));
                    break;

                case Types.JAVA_OBJECT:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialJavaObject(rs.getObject(i)));
                    break;

                case Types.CLOB:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialClob(rs.getClob(i)));
                    break;

                case Types.NCLOB:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialNClob(rs.getNClob(i)));
                    break;

                case Types.BLOB:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialBlob(rs.getBlob(i)));
                    break;

                case Types.ARRAY:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialArray(rs.getArray(i)));
                    break;

                case Types.STRUCT:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialStruct((Struct) rs.getObject(i)));
                    break;

                case ORACLE_ROW_ID:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialRowId(rs.getRowId(i), rs.getString(i)));
                    break;

                    // what oracle does instead of SQLXML in their 1.6 driver,
                    // don't ask me why, commented out so we don't need
                    // an oracle driver to compile this class
                    //case 2007:
                    //_flattenedColumnsValues[internalIndex].setObject(_rowCount, new XMLType(((OracleResultSet)rs).getOPAQUE(i)));
                case Types.SQLXML:
                    _flattenedColumnsValues[internalIndex].setObject(_rowCount, new SerialSQLXML(rs.getSQLXML(i)));
                    break;

                default:
                    if(JavaVersionInfo.use14Api) {
                        if(_columnTypes[internalIndex] == Types.BOOLEAN) {
                            _flattenedColumnsValues[internalIndex].setBoolean(_rowCount, rs.getBoolean(i));
                        }
                        else {
                            foundMatch = false;
                        }
                    } else {
                        foundMatch = false;
                    }
                    break;
                }

                if(foundMatch) {
                    if(rs.wasNull()) {
                        _flattenedColumnsValues[internalIndex].setIsNull(_rowCount);
                    }
                } else {
                    throw new SQLException("Unsupported JDBC-Type: " + _columnTypes[internalIndex]);
                }
            }

            _rowCount++;

        }

        _lastPart = _rowCount < _maxrows; // do not use rs.isAfterLast(), because it is optional for some result sets
        // IMPORTANT, set actual size to _flattenedColumnsValues
        for(int i = _flattenedColumnsValues.length-1; i>=0; i--) {
        	_flattenedColumnsValues[i].setSize(_rowCount);
        }
        return _lastPart;
    }

    private void prepareFlattenedColumns(ResultSetMetaData metaData, int columnCount) throws SQLException {
        _columnTypes = new int[columnCount];
        _flattenedColumnsValues = new ColumnValues[columnCount];
        int initialSize = _maxrows;

        for(int i = 1; i <= columnCount; i++) {
            int columnType = _columnTypes[i - 1] = metaData.getColumnType(i);

            if(_logger.isDebugEnabled()) {
                _logger.debug("Column-Type " + i + ": " + metaData.getColumnType(i));
            }

            Class componentType = null;

            switch (columnType) {
            case Types.NULL:
            	componentType = Object.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);
            	break;
            	
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            	componentType = String.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);
            	break;
            
            case Types.NUMERIC:
            case Types.DECIMAL:
            	if (metaData.getScale(i) == 0) {
            		int precision = metaData.getPrecision(i);
            		if (precision > 0 && precision <= 5) {
            			_columnTypes[i - 1] = Types.SMALLINT;
            			componentType = Short.TYPE;
                    	_flattenedColumnsValues[i - 1] = new ShortColumnValues(initialSize);
                    	break;
            		} else if ((precision > 5 && precision < 39) || (precision == 0)){
            			_columnTypes[i - 1] = Types.INTEGER;
            			componentType = Integer.TYPE;
                    	_flattenedColumnsValues[i - 1] = new IntegerColumnValues(initialSize);
                    	break;
            		}
            	}            	
            	componentType = BigDecimal.class;
            	_flattenedColumnsValues[i - 1] = new BigDecimalColumnValues(initialSize);
            	break;
            case Types.BIT:
                componentType = Boolean.TYPE;
            	_flattenedColumnsValues[i - 1] = new BooleanColumnValues(initialSize);

                break;

            case Types.TINYINT:
                componentType = Byte.TYPE;
            	_flattenedColumnsValues[i - 1] = new ByteColumnValues(initialSize);
                break;

            case Types.SMALLINT:
                componentType = Short.TYPE;
            	_flattenedColumnsValues[i - 1] = new ShortColumnValues(initialSize);                
                break;

            case Types.INTEGER:
                componentType = Integer.TYPE;
            	_flattenedColumnsValues[i - 1] = new IntegerColumnValues(initialSize);
                break;

            case Types.BIGINT:
                componentType = Long.TYPE;
            	_flattenedColumnsValues[i - 1] = new LongColumnValues(initialSize);
                break;

            case Types.REAL:
                componentType = Float.TYPE;
            	_flattenedColumnsValues[i - 1] = new FloatColumnValues(initialSize);
                break;

            case Types.FLOAT:
            case Types.DOUBLE:
                componentType = Double.TYPE;
            	_flattenedColumnsValues[i - 1] = new DoubleColumnValues(initialSize);                
                break;

            case Types.DATE:
            	componentType = java.sql.Date.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);
            	break;
            case Types.TIME:
            	componentType = java.sql.Time.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);
            	break;
            case Types.TIMESTAMP:
            	componentType = java.sql.Timestamp.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            	componentType = byte[].class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            case Types.JAVA_OBJECT:
            	componentType = SerialJavaObject.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            case Types.CLOB:
            	componentType = SerialClob.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            case Types.NCLOB:
            	componentType = SerialNClob.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            case Types.BLOB:
            	componentType = SerialBlob.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);
            	break;
            case Types.ARRAY:
            	componentType = SerialArray.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);
            	break;
            case Types.STRUCT:
            	componentType = SerialStruct.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            case RowPacket.ORACLE_ROW_ID:
            	componentType = SerialRowId.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            case Types.SQLXML:
            	componentType = SerialSQLXML.class;
            	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);            	
            	break;
            	

            default:
                if(JavaVersionInfo.use14Api) {
                    if(columnType == Types.BOOLEAN) {
                        componentType = Boolean.TYPE;
                    	_flattenedColumnsValues[i - 1] = new BooleanColumnValues(initialSize);
                    } else {
                        componentType = Object.class;
                    	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);
                    }
                } else {
                    componentType = Object.class;
                	_flattenedColumnsValues[i - 1] = new ObjectColumnValues(componentType, initialSize);                    
                }
                break;
            }
        }
    }


	int getRowCount() {
		return _rowCount;
	}

	/** necessary for serialization */
	ColumnValues[] getFlattenedColumnsValues() {
		return _flattenedColumnsValues;
	}

	int getIndex() {
		return _index;
	}
}
