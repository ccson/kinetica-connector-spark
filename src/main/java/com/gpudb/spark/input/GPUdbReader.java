package com.gpudb.spark.input;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.RecordObject;
import com.gpudb.protocol.GetRecordsResponse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * GPUdb data processor, used in retrieving records from the database and
 * converting them into object records of the given type
 * 
 * @author dkatz
 * @param <T> Type of object into which GPUdb records will be read
 */
public class GPUdbReader<T extends RecordObject> implements Serializable
{
	private static final long serialVersionUID = 3468971635980373796L;

	private static final Logger log = LoggerFactory.getLogger(GPUdbReader.class);

	/** SparkConf property name for GPUdb connection host */
	public static final String PROP_GPUDB_HOST = "gpudb.host";
	/** SparkConf property name for GPUdb connection port */
	public static final String PROP_GPUDB_PORT = "gpudb.port";
	/** SparkConf property name for GPUdb threads */
	public static final String PROP_GPUDB_THREADS = "gpudb.threads";
	/** SparkConf property name for GPUdb table name to query */
	public static final String PROP_GPUDB_TABLE_NAME = "gpudb.table";

	private static final int DEFAULT_PORT = 9191;
	private static final int DEFAULT_THREADS = 4;

	private String host;
	private int	port;
	private int	threads;
	private String tableName;

	
	/**
	 * Creates a GPUdbReader from the GPUdb configuration parameters in the
	 * given Spark configuration:
	 * <ul>
	 * <li>gpudb.host - hostname/IP of GPUdb server</li>
	 * <li>gpudb.port - port on which GPUdb service is listening</li>
	 * <li>gpudb.threads - number of threads to use in the query</li>
	 * <li>gpudb.tableName - name of table to query</li>
	 * </ul>
	 * 
	 * @param conf Spark configuration containing the GPUdb setup parameters for
	 *        this reader
	 */
	public GPUdbReader(SparkConf conf)
	{
		host = conf.get(PROP_GPUDB_HOST);
		port = conf.getInt(PROP_GPUDB_PORT, DEFAULT_PORT);
		threads = conf.getInt(PROP_GPUDB_THREADS, DEFAULT_THREADS);
		tableName = conf.get(PROP_GPUDB_TABLE_NAME);
		
		if (host == null || host.isEmpty())
			throw new IllegalArgumentException("No GPUdb hostname defined");
		
		if (tableName == null || tableName.isEmpty())
			throw new IllegalArgumentException("No GPUdb table name defined");
	}

	private GPUdb getConn() throws GPUdbException
	{
		return new GPUdb("http://" + host + ":" + port, new GPUdb.Options().setThreadCount(threads));
	}

	/**
	 * Reads the data from a GPUdb table with an optional filter and returns the
	 * data set as an RDD
	 * 
	 * @param recordClass type of object into which each GPUdb record will be
	 *		converted
	 * @param expression optional filter expression
	 * @param context JavaSparkContext used to convert result set to RDD
	 * 
	 * @return RDD of the result data set
	 * @throws GPUdbException if an error occurs reading data from GPUdb
	 */
	public JavaRDD<T> readTable(Class<?> recordClass, String expression, JavaSparkContext context) throws GPUdbException
	{
		Map<String,String> options = new HashMap<String,String>();
		options.put("get_sizes", "true");
		if ((expression != null) && !expression.isEmpty())
			options.put("expression", expression);

		log.debug("Reading GPUdb table <{}> of type <{}>...", tableName, recordClass.getSimpleName());

		GetRecordsResponse<T> response = getConn().getRecords(recordClass, tableName, 0, -9999, null);

		return context.parallelize(response.getData());
	}
}
