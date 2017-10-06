package com.kinetica.util;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.kinetica.exception.KineticaException;
import com.kinetica.metaData.Column;

import java.util.ArrayList;
import java.util.List;

public class TypeUtils {

	public static String getTypeId(Type type) {
		try {
			GPUdb gpuDB = GPUConnectionManager.
				getInstance().getGPUdb();
			return type.create(gpuDB);
		} catch (GPUdbException e) {
			throw new KineticaException(e);
		}
	}

	public static Type getType(String tableName, 
			List<Column> columnInfoList) {
		List<Type.Column> columns = 
			new ArrayList<Type.Column>();

		for (Column columnInfo : columnInfoList) {
			Type.Column column = new Type.Column(
				columnInfo.getName(), columnInfo.getType(), 
					columnInfo.getExtendedTypeProperties());
			columns.add(column);
		}
		return (new Type(tableName, columns));
	}

	public static Type getType(String tableName) {
		try {
			GPUdb gpuDB = GPUConnectionManager.
				getInstance().getGPUdb();
			return Type.fromTable(gpuDB, tableName);
		} catch (GPUdbException e) {
			throw new KineticaException(e);
		}		
	}
}