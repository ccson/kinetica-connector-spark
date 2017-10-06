package com.kinetica.metaData;

import java.util.List;

public class Column {

	private int columnNo;
	private String name;
	private Class<?> type;
	private List<String> 
		extendedTypeProperties;
	private boolean timestamp = false;

	public Column() {
	}

	public Column(String name, 
			Class<?> type, List<String> 
				extendedTypeProperties) {
		super();
		this.name = name;
		this.type = type;
		this.extendedTypeProperties 
			= extendedTypeProperties;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Class<?> getType() {
		return type;
	}
	public void setType(Class<?> type) {
		this.type = type;
	}
	public List<String> getExtendedTypeProperties() {
		return extendedTypeProperties;
	}
	public void setExtendedTypeProperties(
			List<String> extendedTypeProperties) {
		this.extendedTypeProperties = extendedTypeProperties;
	}

	public boolean isTimestamp() {
		return timestamp;
	}

	public void setTimestamp(boolean timestamp) {
		this.timestamp = timestamp;
	}

	public int getColumnNo() {
		return columnNo;
	}

	public void setColumnNo(int columnNo) {
		this.columnNo = columnNo;
	}
}