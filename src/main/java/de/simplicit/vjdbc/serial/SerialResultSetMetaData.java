// VJDBC - Virtual JDBC
// Written by Michael Link
// Website: http://vjdbc.sourceforge.net

package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class SerialResultSetMetaData implements ResultSetMetaData, Externalizable, KryoSerializable {
	private static final int CATALOG_NAME_ERROR_MASK = 1;
	private static final int SCHEMA_NAME_ERROR_MASK = CATALOG_NAME_ERROR_MASK<<1;
	private static final int TABLE_NAME_ERROR_MASK = SCHEMA_NAME_ERROR_MASK<<1;
	private static final int COLUMN_CLASS_NAME_ERROR_MASK = TABLE_NAME_ERROR_MASK<<1;
	private static final int COLUMN_LABEL_ERROR_MASK = COLUMN_CLASS_NAME_ERROR_MASK<<1;	
	private static final int COLUMN_NAME_ERROR_MASK = COLUMN_LABEL_ERROR_MASK<<1;
	private static final int COLUMN_TYPE_NAME_ERROR_MASK = COLUMN_NAME_ERROR_MASK<<1;
	
	private static final int COLUMN_TYPE_ERROR_MASK = COLUMN_TYPE_NAME_ERROR_MASK<<1;
	private static final int COLUMN_DISPLAY_SIZE_ERROR_MASK = COLUMN_TYPE_ERROR_MASK<<1;
	private static final int PRECISION_ERROR_MASK = COLUMN_DISPLAY_SIZE_ERROR_MASK<<1;
    private static final int SCALE_ERROR_MASK = PRECISION_ERROR_MASK<<1;
    private static final int NULLABLE_ERROR_MASK = SCALE_ERROR_MASK<<1;
    
    private static final int AUTO_INCREMENT_ERROR_MASK = NULLABLE_ERROR_MASK<<1;
    private static final int CASE_SENSITIVE_ERROR_MASK = AUTO_INCREMENT_ERROR_MASK<<1;
    private static final int CURRENCY_ERROR_MASK = CASE_SENSITIVE_ERROR_MASK<<1;
    private static final int READ_ONLY_ERROR_MASK = CURRENCY_ERROR_MASK<<1;
    private static final int SEARCHABLE_ERROR_MASK = READ_ONLY_ERROR_MASK<<1;
    private static final int SIGNED_ERROR_MASK = SEARCHABLE_ERROR_MASK<<1;
    private static final int WRITABLE_ERROR_MASK = SIGNED_ERROR_MASK<<1;
    private static final int DEFINITIVELY_WRITABLE_ERROR_MASK = WRITABLE_ERROR_MASK<<1;
	

	static final long serialVersionUID = 9034215340975782405L;


    private int _columnCount;

    private String[] _catalogName;
    private String[] _schemaName;
    private String[] _tableName;
    private String[] _columnClassName;
    private String[] _columnLabel;
    private String[] _columnName;
    private String[] _columnTypeName;

    private int[] _columnType;
    private int[] _columnDisplaySize;
    private int[] _precision;
    private int[] _scale;
    private int[] _nullable;

    private boolean[] _autoIncrement;
    private boolean[] _caseSensitive;
    private boolean[] _currency;
    private boolean[] _readOnly;
    private boolean[] _searchable;
    private boolean[] _signed;
    private boolean[] _writable;
    private boolean[] _definitivelyWritable;
    
    private int[] _error;

    public SerialResultSetMetaData() {
    }

    public SerialResultSetMetaData(ResultSetMetaData rsmd) throws SQLException {
        _columnCount = rsmd.getColumnCount();

        allocateArrays();
        fillArrays(rsmd);
    }

    public String[] readStringArr(ObjectInput in) throws IOException
    {
        int numElems = in.readShort();
        if (numElems != -1) {
            String ret[] = new String[numElems];
            for (int i = 0; i < numElems; i++) {
                byte notNull = in.readByte();
                if (1 == notNull) {
                    ret[i] = in.readUTF();
                } else {
                    ret[i] = null;
                }
            }
            return ret;
        }
        return null;
    }

    public void writeStringArr(String arr[], ObjectOutput out)
        throws IOException
    {
        if (arr != null) {
            out.writeShort(arr.length);
            for (int i = 0; i < arr.length; i++) {
                if (arr[i] != null) {
                    out.writeByte(1);
                    out.writeUTF(arr[i]);
                } else {
                    out.writeByte(0);
                }
            }
        } else {
            out.writeShort(-1);
        }
    }

    public int[] readIntArr(ObjectInput in) throws IOException
    {
        int numElems = in.readShort();
        if (numElems != -1) {
            int ret[] = new int[numElems];
            for (int i = 0; i < numElems; i++) {
                ret[i] = in.readInt();
            }
            return ret;
        }
        return null;
    }

    public void writeIntArr(int arr[], ObjectOutput out)
        throws IOException
    {
        if (arr != null) {
            out.writeShort(arr.length);
            for (int i = 0; i < arr.length; i++) {
                out.writeInt(arr[i]);
            }
        } else {
            out.writeShort(-1);
        }
    }

    public boolean[] readBooleanArr(ObjectInput in) throws IOException
    {
        int numElems = in.readShort();
        if (numElems != -1) {
        	boolean ret[] = new boolean[numElems];
            for (int i = 0; i < numElems; i++) {
                ret[i] = in.readBoolean();
            }
            return ret;
        }
        return null;
    }

    public void writeBooleanArr(boolean arr[], ObjectOutput out)
        throws IOException
    {
        if (arr != null) {
            out.writeShort(arr.length);
            for (int i = 0; i < arr.length; i++) {
                out.writeBoolean(arr[i]);
            }
        } else {
            out.writeShort(-1);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _columnCount = in.readInt();

        _error = readIntArr(in);
        
        _catalogName = readStringArr(in);
        _schemaName = readStringArr(in);
        _tableName = readStringArr(in);
        _columnClassName = readStringArr(in);
        _columnLabel = readStringArr(in);
        _columnName = readStringArr(in);
        _columnTypeName = readStringArr(in);

        _columnType = readIntArr(in);
        _columnDisplaySize = readIntArr(in);
        _precision = readIntArr(in);
        _scale = readIntArr(in);
        _nullable = readIntArr(in);

        _autoIncrement = readBooleanArr(in);
        _caseSensitive = readBooleanArr(in);
        _currency = readBooleanArr(in);
        _readOnly = readBooleanArr(in);
        _searchable = readBooleanArr(in);
        _signed = readBooleanArr(in);
        _writable = readBooleanArr(in);
        _definitivelyWritable = readBooleanArr(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_columnCount);

        writeIntArr(_error, out);
        
        writeStringArr(_catalogName, out);
        writeStringArr(_schemaName, out);
        writeStringArr(_tableName, out);
        writeStringArr(_columnClassName, out);
        writeStringArr(_columnLabel, out);
        writeStringArr(_columnName, out);
        writeStringArr(_columnTypeName, out);

        writeIntArr(_columnType, out);
        writeIntArr(_columnDisplaySize, out);
        writeIntArr(_precision, out);
        writeIntArr(_scale, out);
        writeIntArr(_nullable, out);

        writeBooleanArr(_autoIncrement, out);
        writeBooleanArr(_caseSensitive, out);
        writeBooleanArr(_currency, out);
        writeBooleanArr(_readOnly, out);
        writeBooleanArr(_searchable, out);
        writeBooleanArr(_signed, out);
        writeBooleanArr(_writable, out);
        writeBooleanArr(_definitivelyWritable, out);
    }

    private void allocateArrays() {
        _catalogName = new String[_columnCount];
        _schemaName = new String[_columnCount];
        _tableName = new String[_columnCount];
        _columnClassName = new String[_columnCount];
        _columnLabel = new String[_columnCount];
        _columnName = new String[_columnCount];
        _columnTypeName = new String[_columnCount];

        _columnDisplaySize = new int[_columnCount];
        _columnType = new int[_columnCount];
        _precision = new int[_columnCount];
        _scale = new int[_columnCount];
        _nullable = new int[_columnCount];

        _autoIncrement = new boolean[_columnCount];
        _caseSensitive = new boolean[_columnCount];
        _currency = new boolean[_columnCount];
        _readOnly = new boolean[_columnCount];
        _searchable = new boolean[_columnCount];
        _signed = new boolean[_columnCount];
        _writable = new boolean[_columnCount];
        _definitivelyWritable = new boolean[_columnCount];
        
        _error = new int[_columnCount];
    }

    private void fillArrays(ResultSetMetaData rsmd) {
        for(int i = 0; i < _columnCount; i++) {
            int col = i + 1;
            int e = 0;
            try {
                _catalogName[i] = rsmd.getCatalogName(col);
            } catch(Exception e0) {
                _catalogName[i] = null;
                e |= CATALOG_NAME_ERROR_MASK;
            }

            try {
                _schemaName[i] = rsmd.getSchemaName(col);
            } catch(Exception e1) {
                _schemaName[i] = null;
                e |= SCHEMA_NAME_ERROR_MASK;
            }
            
            try {
                _tableName[i] = rsmd.getTableName(col);
            } catch(Exception e2) {
                _tableName[i] = null;
                e |= TABLE_NAME_ERROR_MASK;
            }

            try {
                _columnLabel[i] = rsmd.getColumnLabel(col);
            } catch(Exception e3) {
                _columnLabel[i] = null;
                e |= COLUMN_LABEL_ERROR_MASK;
            }

            try {
                _columnName[i] = rsmd.getColumnName(col);
            } catch(Exception e4) {
                _columnName[i] = null;
                e |= COLUMN_NAME_ERROR_MASK;
            }

            try {
                _columnClassName[i] = rsmd.getColumnClassName(col);
            } catch(Exception e5) {
                _columnClassName[i] = null;
                e |= COLUMN_CLASS_NAME_ERROR_MASK;
            }

            try {
                _columnTypeName[i] = rsmd.getColumnTypeName(col);
            } catch(Exception e6) {
                _columnTypeName[i] = null;
                e |= COLUMN_TYPE_NAME_ERROR_MASK;
            }

            try {
                _columnDisplaySize[i] = rsmd.getColumnDisplaySize(col);
            } catch(Exception e7) {
                e |= COLUMN_DISPLAY_SIZE_ERROR_MASK;
            }

            try {
                _columnType[i] = rsmd.getColumnType(col);
            } catch(Exception e8) {
                e |= COLUMN_TYPE_ERROR_MASK;
            }

            try {
                _precision[i] = rsmd.getPrecision(col);
            } catch(Exception e9) {
                e |= PRECISION_ERROR_MASK;
            }

            try {
                _scale[i] = rsmd.getScale(col);
            } catch(Exception e10) {
                e |= SCALE_ERROR_MASK;
            }

            try {
                _nullable[i] = rsmd.isNullable(col);
            } catch(Exception e11) {
                e |= NULLABLE_ERROR_MASK;
            }

            try {
                _autoIncrement[i] = rsmd.isAutoIncrement(col);
            } catch(Exception e12) {
                e |= AUTO_INCREMENT_ERROR_MASK;
            }

            try {
                _caseSensitive[i] = rsmd.isCaseSensitive(col);
            } catch(Exception e13) {
                e |= CASE_SENSITIVE_ERROR_MASK;
            }

            try {
                _currency[i] = rsmd.isCurrency(col);
            } catch(Exception e14) {
                e |= CURRENCY_ERROR_MASK;
            }

            try {
                _readOnly[i] = rsmd.isReadOnly(col);
            } catch(Exception e15) {
                e |= READ_ONLY_ERROR_MASK;
            }

            try {
                _searchable[i] = rsmd.isSearchable(col);
            } catch(Exception e16) {
                e |= SEARCHABLE_ERROR_MASK;
            }

            try {
                _signed[i] = rsmd.isSigned(col);
            } catch(Exception e17) {
                e |= SIGNED_ERROR_MASK;
            }

            try {
                _writable[i] = rsmd.isWritable(col);
            } catch(Exception e18) {
                e |= WRITABLE_ERROR_MASK;
            }

            try {
                _definitivelyWritable[i] = rsmd.isDefinitelyWritable(col);
            } catch(Exception e18) {
                e |= DEFINITIVELY_WRITABLE_ERROR_MASK;
            }
         
            _error[i] = e;
        }
    }

    public int getColumnCount() throws SQLException {
        return _columnCount;
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & AUTO_INCREMENT_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _autoIncrement[column - 1];
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & CASE_SENSITIVE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _caseSensitive[column - 1];
    }

    public boolean isSearchable(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & SEARCHABLE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }        
        return _searchable[column - 1];
    }

    public boolean isCurrency(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & CURRENCY_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }        
        return _currency[column - 1];
    }

    public int isNullable(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & NULLABLE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }       
        return _nullable[column - 1];
    }

    public boolean isSigned(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & SIGNED_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _signed[column - 1];
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & COLUMN_DISPLAY_SIZE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _columnDisplaySize[column - 1];
    }

    public String getColumnLabel(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & COLUMN_LABEL_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }        
        return _columnLabel[column - 1];
    }

    public String getColumnName(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & COLUMN_NAME_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _columnName[column - 1];
    }

    public String getSchemaName(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & SCHEMA_NAME_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _schemaName[column - 1];
    }

    public int getPrecision(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & PRECISION_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }        
        return _precision[column - 1];
    }

    public int getScale(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & SCALE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _scale[column - 1];
    }

    public String getTableName(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & TABLE_NAME_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }        
        return _tableName[column - 1];
    }

    public String getCatalogName(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & CATALOG_NAME_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }        
        return _catalogName[column - 1];
    }

    public int getColumnType(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & COLUMN_TYPE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _columnType[column - 1];
    }

    public String getColumnTypeName(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & COLUMN_TYPE_NAME_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _columnTypeName[column - 1];
    }

    public boolean isReadOnly(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & READ_ONLY_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _readOnly[column - 1];
    }

    public boolean isWritable(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & WRITABLE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _writable[column - 1];
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & DEFINITIVELY_WRITABLE_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _definitivelyWritable[column - 1];
    }

    public String getColumnClassName(int column) throws SQLException {
        checkColumnIndex(column);
        if ((_error[column-1] & COLUMN_CLASS_NAME_ERROR_MASK)!=0){
        	throw new SQLException("Error on server side");
        }
        return _columnClassName[column - 1];
    }

    public void setColumnCount(int columnCount) throws SQLException {
        if (columnCount < 0) {
            throw new SQLException("invalid number of columns " + columnCount);
        }
        _columnCount = columnCount;
        allocateArrays();
    }

    public void setAutoIncrement(int columnIndex, boolean property)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _autoIncrement[columnIndex - 1] = property;
    }

    public void setCaseSensitive(int columnIndex, boolean property)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _caseSensitive[columnIndex - 1] = property;
    }

    public void setCatalogName(int columnIndex, String catalogName)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _catalogName[columnIndex - 1] = catalogName;
    }

    public void setColumnDisplaySize(int columnIndex, int size)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _columnDisplaySize[columnIndex - 1] = size;
    }

    public void setColumnLabel(int columnIndex, String label)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _columnLabel[columnIndex - 1] = label;
    }

    public void setColumnName(int columnIndex, String columnName)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _columnName[columnIndex - 1] = columnName;
    }

    public void setColumnType(int columnIndex, int SQLType)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _columnType[columnIndex - 1] = SQLType;
    }

    public void setColumnTypeName(int columnIndex, String typeName)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _columnTypeName[columnIndex - 1] = typeName;
    }

    public void setCurrency(int columnIndex, boolean property)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _currency[columnIndex - 1] = property;
    }

    public void setNullable(int columnIndex, int property)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _nullable[columnIndex - 1] = property;
    }

    public void setPrecision(int columnIndex, int precision)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _precision[columnIndex - 1] = precision;
    }

    public void setScale(int columnIndex, int scale)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _scale[columnIndex - 1] = scale;
    }

    public void setSchemaName(int columnIndex, String schemaName)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _schemaName[columnIndex - 1] = schemaName;
    }

    public void setSearchable(int columnIndex, boolean property)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _searchable[columnIndex - 1] = property;
    }

    public void setSigned(int columnIndex, boolean property)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _signed[columnIndex - 1] = property;
    }

    public void setTableName(int columnIndex, String tableName)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _tableName[columnIndex - 1] = tableName;
    }

    public void setReadOnly(int columnIndex, boolean readOnly)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _readOnly[columnIndex - 1] = readOnly;
    }

    public void setWritable(int columnIndex, boolean writable)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _writable[columnIndex - 1] = writable;
    }

    public void setDefinitelyWritable(int columnIndex, boolean writable)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _definitivelyWritable[columnIndex - 1] = writable;
    }

    public void setColumnClassName(int columnIndex, String columnClassName)
        throws SQLException {
        checkColumnIndex(columnIndex);
        _columnClassName[columnIndex - 1] = columnClassName;
    }

    private void checkColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > _columnCount) {
            throw new SQLException("invalid column index " + columnIndex);
        }
    }

    /* start JDBC4 support */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(SerialResultSetMetaData.class);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)this;
    }
    /* end JDBC4 support */

	@Override
	public void write(Kryo kryo, Output output) {
		output.writeInt(_columnCount);

		kryo.writeObjectOrNull(output, _error, int[].class);
		
		kryo.writeObjectOrNull(output, _catalogName, String[].class);
		kryo.writeObjectOrNull(output, _schemaName, String[].class);
		kryo.writeObjectOrNull(output, _tableName, String[].class);
		kryo.writeObjectOrNull(output, _columnClassName, String[].class);
		kryo.writeObjectOrNull(output, _columnLabel, String[].class);
		kryo.writeObjectOrNull(output, _columnName, String[].class);
		kryo.writeObjectOrNull(output, _columnTypeName, String[].class);

		kryo.writeObjectOrNull(output, _columnType, int[].class);
		kryo.writeObjectOrNull(output, _columnDisplaySize, int[].class);
		kryo.writeObjectOrNull(output, _precision, int[].class);
		kryo.writeObjectOrNull(output, _scale, int[].class);
		kryo.writeObjectOrNull(output, _nullable, int[].class);

		kryo.writeObjectOrNull(output, _autoIncrement, boolean[].class);
		kryo.writeObjectOrNull(output, _caseSensitive, boolean[].class);
		kryo.writeObjectOrNull(output, _currency, boolean[].class);
		kryo.writeObjectOrNull(output, _readOnly, boolean[].class);
		kryo.writeObjectOrNull(output, _searchable, boolean[].class);
		kryo.writeObjectOrNull(output, _signed, boolean[].class);
		kryo.writeObjectOrNull(output, _writable, boolean[].class);
		kryo.writeObjectOrNull(output, _definitivelyWritable, boolean[].class);

	}

	@Override
	public void read(Kryo kryo, Input input) {
		_columnCount = input.readInt();

	    _error = kryo.readObjectOrNull(input, int[].class);

	    _catalogName = kryo.readObjectOrNull(input, String[].class);
	    _schemaName = kryo.readObjectOrNull(input, String[].class);
	    _tableName = kryo.readObjectOrNull(input, String[].class);
	    _columnClassName = kryo.readObjectOrNull(input, String[].class);
	    _columnLabel = kryo.readObjectOrNull(input, String[].class);
	    _columnName = kryo.readObjectOrNull(input, String[].class);
	    _columnTypeName = kryo.readObjectOrNull(input, String[].class);

	    _columnType = kryo.readObjectOrNull(input, int[].class);
	    _columnDisplaySize = kryo.readObjectOrNull(input, int[].class);
	    _precision = kryo.readObjectOrNull(input, int[].class);
	    _scale = kryo.readObjectOrNull(input, int[].class);
	    _nullable = kryo.readObjectOrNull(input, int[].class);

	    _autoIncrement = kryo.readObjectOrNull(input, boolean[].class);
	    _caseSensitive = kryo.readObjectOrNull(input, boolean[].class);
	    _currency = kryo.readObjectOrNull(input, boolean[].class);
	    _readOnly = kryo.readObjectOrNull(input, boolean[].class);
	    _searchable = kryo.readObjectOrNull(input, boolean[].class);
	    _signed = kryo.readObjectOrNull(input, boolean[].class);
	    _writable = kryo.readObjectOrNull(input, boolean[].class);
	    _definitivelyWritable = kryo.readObjectOrNull(input, boolean[].class);
	}

	String[] getColumnLabels() {
		return _columnLabel;
	}

	String[] getColumnNames() {
		return _columnName;
	}

	int[] getColumnTypes() {
		return _columnType;
	}
}

