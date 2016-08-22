package com.gpudb.spark.input;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.RecordObject;
import com.gpudb.protocol.FilterResponse;
import com.gpudb.protocol.GetRecordsResponse;
import com.gpudb.protocol.ShowSystemPropertiesRequest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

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
	/** SparkConf property name for GPUdb table name to query */
	public static final String PROP_GPUDB_READ_SIZE = "gpudb.read.size";
	
	/** GPUdb server-side record retrieval hard limit */
	private static final String SERVER_ENV_MAX_GET_RECORDS_SIZE = "conf.max_get_records_size";

	private static final int DEFAULT_PORT = 9191;
	private static final int DEFAULT_THREADS = 4;
	private static final int DEFAULT_READ_SIZE = 10000;

	private String host;
	private int	port;
	private int	threads;
	private String tableName;
	private int	readSize;
	private int	readSizeConfigured;

	
	/**
	 * Creates a {@code GPUdbReader} from the GPUdb configuration parameters in
	 * the given Spark configuration:
	 * <ul>
	 * <li>gpudb.host - hostname/IP of GPUdb server</li>
	 * <li>gpudb.port - port on which GPUdb service is listening</li>
	 * <li>gpudb.threads - number of threads to use in the query</li>
	 * <li>gpudb.tableName - name of table to query</li>
	 * <li>gpudb.read.size - size of blocks of data to read per request</li>
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
		readSizeConfigured = conf.getInt(PROP_GPUDB_READ_SIZE, DEFAULT_READ_SIZE);
		
		if (host == null || host.isEmpty())
			throw new IllegalArgumentException("No GPUdb hostname defined");
		
		if (tableName == null || tableName.isEmpty())
			throw new IllegalArgumentException("No GPUdb table name defined");
	}

	private GPUdb getConn() throws GPUdbException
	{
		GPUdb gpudb = new GPUdb("http://" + host + ":" + port, new GPUdb.Options().setThreadCount(threads));

		// If readSize is not set yet, compare the configured size and the
		//   server-side hard limit, to determine which is smaller; necessary to
		//   partition record requests into valid offset/limit ranges
		if (readSize == 0)
		{
			readSize = readSizeConfigured;
			
			String maxReadSizeEnv = gpudb.showSystemProperties(GPUdb.options(ShowSystemPropertiesRequest.Options.PROPERTIES, SERVER_ENV_MAX_GET_RECORDS_SIZE)).getPropertyMap().get(SERVER_ENV_MAX_GET_RECORDS_SIZE);

			try
			{
				int maxReadSize = Integer.parseInt(maxReadSizeEnv);

				if (maxReadSize < readSize)
					readSize = maxReadSize;
			}
			catch (Exception e)
			{
				log.error("Unable to parse GPUdb environment setting <{}>", SERVER_ENV_MAX_GET_RECORDS_SIZE);
			}
		}

		return gpudb;
	}


	/**
	 * Reads the data from a GPUdb table with an optional filter and returns the
	 * data set as an RDD.  The database requests will be partitioned in batches
	 * of size configured in the {@code SparkConf} used to create this
	 * {@code GPUdbReader}, or of size configured as the server-side limit,
	 * whichever is smaller.
	 * 
	 * Thus, given the following setup:
	 * <ul>
	 * <li>a configured read size of 30,000</li>
	 * <li>a server-side configured hard record retrieval limit of 20,000</li>
	 * <li>a record set of size 100,000 to retrieve</li>
	 * </ul>
	 * Five requests for 20,000 records each will be partitioned and made.
	 * 
	 * This method will create a temporary filter view, whether an expression is
	 * given or not, named {@code [tableName].[RandomUUID]}.
	 * 
	 * @param recordClass type of object into which each GPUdb record will be
	 *		converted
	 * @param expression optional filter expression
	 * @param context JavaSparkContext used to convert result set to RDD
	 * 
	 * @return RDD of the result data set
	 * @throws GPUdbException if an error occurs reading data from GPUdb
	 */
	public JavaRDD<T> readTable(final Class<?> recordClass, String expression, JavaSparkContext context) throws GPUdbException
	{
		final String viewName = tableName + "." + UUID.randomUUID().toString();
		final Map<String,String> options = new HashMap<String,String>();
		if ((expression != null) && !expression.isEmpty())
			options.put("expression", expression);

		log.info("Creating filter <{}> of GPUdb table <{}> of type <{}>...", viewName, tableName, recordClass.getSimpleName());

		FilterResponse filterResponse = getConn().filter(tableName, viewName, expression, null);

		long totalRecords = filterResponse.getCount();

		// Create a set of offsets for getRecords calls that can be partitioned
		//   and executed in parallel (readSize should have been adjusted in the
		//   getConn() call above and provide a valid batch limit)
		List<Long> offsetList = new ArrayList<Long>();

		for (long offset = 0; offset < totalRecords; offset += readSize)
			offsetList.add(offset);

		JavaRDD<Long> rdd = context.parallelize(offsetList);

		// Translate each offset into the records in the block starting from
		//   that offset and containing up to readSize records 
		return rdd.flatMap
		(
			new FlatMapFunction<Long,T>()
			{
				private static final long serialVersionUID = 1519062387719363984L;

				@Override
				public List<T> call(Long offset) throws GPUdbException
				{
					log.info("Reading GPUdb view <{}> records <{}>-<{}>...", viewName, offset, offset + readSize - 1);

					GetRecordsResponse<T> response = getConn().getRecords(recordClass, viewName, offset, readSize, options);
					return response.getData();
				}
			}
		);
	}
}
