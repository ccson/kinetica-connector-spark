package com.kinetica.util;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.kinetica.exception.KineticaException;
import com.kinetica.spark.util.LoaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GPUConnectionManager {

	private static GPUConnectionManager instance = null;
	private static GPUdb gpudb = null;
	private final static Logger logger =
			LoggerFactory.getLogger(GPUConnectionManager.class);

	private static String GPUDB_CONNECT_STRING;
	private static boolean ENABLE_AUTHENTICATION;
	private static String USERNAME;
	private static String PASSWORD;
	private static int PROP_GPUDB_THREADS;

	private static void GPUConnectionManager(LoaderParams lp){
		GPUDB_CONNECT_STRING = lp.getGPUdbURL();
		ENABLE_AUTHENTICATION = lp.getKauth();
		USERNAME = lp.getKusername();
		PASSWORD = lp.getKpassword();
		PROP_GPUDB_THREADS = lp.getThreads();

	}

	private GPUConnectionManager() {
		logger.info("Connection to Kinetica at: " + GPUDB_CONNECT_STRING);
		if (gpudb == null) {
			try {
				GPUdbBase.Options opts = getGPUDBOptions();
				gpudb = new GPUdb(GPUDB_CONNECT_STRING, opts);
			} catch (Exception e) {
				throw new KineticaException(e);
			}
		}
	}

	private GPUdbBase.Options getGPUDBOptions() {
		GPUdbBase.Options opts = new GPUdb.Options();
		logger.info("Authentication enabled is: " + ENABLE_AUTHENTICATION);
		if (ENABLE_AUTHENTICATION) {
			opts.setUsername(USERNAME.trim());
			opts.setPassword(PASSWORD.trim());
		}
		opts.setThreadCount(PROP_GPUDB_THREADS) ;
		return opts;
	}

	public static GPUConnectionManager getInstance() {
		if (instance == null) {
//			synchronized (instance) {
//				if (instance == null) {
					instance = new GPUConnectionManager();
//				}	
//			}
		}
		return instance;
	}
	
	public GPUdb getGPUdb() {
		if (gpudb == null || instance == null)
			throw new KineticaException("Please call getInstance first.");
		return gpudb;
	}
}