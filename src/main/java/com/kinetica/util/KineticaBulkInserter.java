package com.kinetica.util;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.protocol.InsertRecordsRequest;
import com.kinetica.exception.KineticaException;
import com.kinetica.metaData.Table;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class KineticaBulkInserter {

	private static KineticaBulkInserter instance = null;
	private static BulkInserter<IndexedRecord> bulkInserter = null;
	private final static Logger logger =
			LoggerFactory.getLogger(BulkInserter.class);
	private static Table table = null;
	private static GPUdb gpudb = null;

	private KineticaBulkInserter() {	
		try {
			gpudb = GPUConnectionManager.getInstance().getGPUdb();
			BulkInserter.WorkerList workers = getWorkers(table);
			
			bulkInserter = new BulkInserter
				<IndexedRecord>(gpudb, table.getName(), table.getType(), 
					KineticaConfiguration.BULKINGESTOR_BATCH_SIZE, getInsertUpdateOptions(), workers);
		} catch (Exception e) {
			throw new KineticaException(e);
		}
	}

	private BulkInserter.WorkerList getWorkers(Table table) throws GPUdbException {
		BulkInserter.WorkerList workers = null;
		if (!table.isReplicated()) {
			if ((KineticaConfiguration.IP_REGEX != null)
					&& !(KineticaConfiguration.IP_REGEX.trim().equalsIgnoreCase(""))) {
				Pattern pattern = Pattern.compile(KineticaConfiguration.IP_REGEX);
				workers = new BulkInserter.WorkerList(gpudb, pattern);
			} else {
				workers = new BulkInserter.WorkerList(gpudb);
			}
			logger.info("Number of workers: " + workers.size());
			for (Iterator<URL> iter = workers.iterator(); iter.hasNext();) {
				logger.info("GPUdb BulkInserter worker: " + iter.next());
			}
		}
		return workers;
	}
	
	private Map<String, String> getInsertUpdateOptions() {
		Map<String,String> options = new HashMap<String, String>();
		if (KineticaConfiguration.UPDATE_ON_EXISTING_PK) {
			options.put("update_on_existing_pk", 
				InsertRecordsRequest.Options.TRUE);
		} else {
			options.put("update_on_existing_pk", 
				InsertRecordsRequest.Options.FALSE);			
		}
		return options;
	}
	
	public static BulkInserter<IndexedRecord> getBulkInserter() {	
		return bulkInserter;
	}

	public static synchronized KineticaBulkInserter getInstance(Table value) {
		table = value;
		if (instance == null) {
			instance = new KineticaBulkInserter();
		}
		return instance;
	}
}