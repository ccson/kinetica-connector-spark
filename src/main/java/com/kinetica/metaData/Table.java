package com.kinetica.metaData;

import com.gpudb.Type;

import com.kinetica.util.KineticaConfiguration;


public class Table {

	private Type type;
	private String typeId;

	public Table(Type
			type, String typeId) {
		this.type = type;
		this.typeId = typeId;
	}

	public String getCollectionName() {
		if (KineticaConfiguration.COLLECTION_NAME != null)
			return KineticaConfiguration.COLLECTION_NAME.trim();
		return null;
	}
	public String getName() {
		return KineticaConfiguration.TABLE_NAME.trim();
	}
	public String getTypeId() {
		return typeId;
	}
	public void setTypeId(String typeId) {
		this.typeId = typeId;
	}

	public boolean isReplicated() {
		return KineticaConfiguration.IS_REPLICATED;
	}
	public boolean isDeleteTableIfExists() {
		return KineticaConfiguration.DELETE_TABLE_IF_EXISTS;
	}
	public Type getType() {
		return type;
	}
	public void setType(Type type) {
		this.type = type;
	}
}
