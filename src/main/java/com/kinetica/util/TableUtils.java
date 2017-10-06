package com.kinetica.util;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.ClearTableRequest;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.HasTableRequest;
import com.kinetica.exception.KineticaException;
import com.kinetica.metaData.Column;
import com.kinetica.metaData.Table;
import com.kinetica.spark.util.LoaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableUtils {

	private final static Logger logger =
		LoggerFactory.getLogger(TableUtils.class);

	private static LoaderParams lp;
	
	private static final GPUdb connection = 
		GPUConnectionManager.getInstance().getGPUdb();

	public static void Init(LoaderParams ilp){
		lp = ilp;
		JDBCConnectionUtils.Init(lp.getJdbcURL());
	}
	
	public synchronized static void dropTableIfExists(Table table) {
		String tableName = table.getName();
		if (tableExists(tableName) && 
				table.isDeleteTableIfExists()) {
			logger.info("Table " + tableName 
				+ " exists - dropping the table.");
			dropTable(tableName);
		}

	}

	
	public static boolean tableExists(String tableName) {
		HasTableRequest request = new HasTableRequest(tableName, null);
		try {
			return connection.hasTable(request).getTableExists();
		} catch (GPUdbException e) {
			throw new KineticaException(e);
		}
	}

	public static void dropTable(String tableName) {
		logger.info("Dropping table " + tableName);
		ClearTableRequest request = new 
			ClearTableRequest(tableName, null, null);		
		try {
			connection.clearTable(request);
		} catch (GPUdbException e) {
			throw new KineticaException(e);
		}
	}

	private static void executeSQL(String sql) {
		JDBCConnectionUtils.executeSQL(sql);

	}

	public static void truncate(String tableName) {
		List<String> expressions = new ArrayList<String>();
		expressions.add("1 = 1");
		try {
			connection.deleteRecords(tableName, expressions, null);
		} catch (GPUdbException e) {
			throw new KineticaException(e);
		}
	}

	public static Table getTable(String tableName) {
		Type recordType = TypeUtils.getType(tableName);
		String typeId = TypeUtils.getTypeId(recordType);
		return new Table(recordType, typeId);
	}

	public static List<Column> getTimeStampColumns(Type type) {
		List<Column> timestampColumns = new ArrayList<Column>();
		List<Type.Column> columnList = type.getColumns();
		int columnNo = 0;
		for (Type.Column column : columnList) {
			Column kineticaColumn = new Column();
			kineticaColumn.setColumnNo(columnNo);
			kineticaColumn.setName(column.getName());
			kineticaColumn.setExtendedTypeProperties(column.getProperties());
			kineticaColumn.setType(column.getClass());

			if (isTimeStamp(column.getProperties())) {
				kineticaColumn.setTimestamp(true);
				timestampColumns.add(kineticaColumn);
			}
			columnNo++;
		}
		return timestampColumns;
	}
	
	private static boolean isTimeStamp(List<String> properties) {
		for (String property : properties) {
			if (property.contains("timestamp")) {
				return true;
			}
		}
		return false;
	}
}