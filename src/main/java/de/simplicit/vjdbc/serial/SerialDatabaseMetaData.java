package de.simplicit.vjdbc.serial;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class SerialDatabaseMetaData implements Externalizable {

	private UIDEx _objectId;
	
	private String databaseProductName;
	private String databaseProductVersion;
	private String driverName;
	private String driverVersion;
		
	public SerialDatabaseMetaData() {
	}

	
	public SerialDatabaseMetaData(UIDEx objectId, String databaseProductName, String databaseProductVersion,
			String driverName, String driverVersion) {
		this._objectId = objectId;
		this.databaseProductName = databaseProductName;
		this.databaseProductVersion = databaseProductVersion;
		this.driverName = driverName;
		this.driverVersion = driverVersion;
	}

	public SerialDatabaseMetaData(UIDEx objectId, DatabaseMetaData databaseMetaData) throws SQLException {
		this._objectId = objectId;
		this.databaseProductName = databaseMetaData.getDatabaseProductName();
		this.databaseProductVersion = databaseMetaData.getDatabaseProductVersion();
		this.driverName = databaseMetaData.getDriverName();
		this.driverVersion = databaseMetaData.getDriverVersion();		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeObject(_objectId);
		out.writeUTF(databaseProductName);
		out.writeUTF(databaseProductVersion);
		out.writeUTF(driverName);
		out.writeUTF(driverVersion);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		_objectId = (UIDEx) in.readObject();
		databaseProductName = in.readUTF();
		databaseProductVersion = in.readUTF();
		driverName = in.readUTF();
		driverVersion = in.readUTF();
	}


	public UIDEx getUIDEx() {
		return _objectId;
	}

	public String getDatabaseProductName() {
		return databaseProductName;
	}


	public String getDatabaseProductVersion() {
		return databaseProductVersion;
	}


	public String getDriverName() {
		return driverName;
	}


	public String getDriverVersion() {
		return driverVersion;
	}
}
