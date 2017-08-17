package com.gpudb.spark;

import com.gpudb.BulkInserter;
import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.GenericRecord;
import com.gpudb.Type;
import com.gpudb.spark.dao.AvroWrapper;
import com.gpudb.spark.input.GPUdbReceiver;
import com.gpudb.spark.output.GPUdbWriter;
import com.gpudb.spark.util.GPUdbUtil;
import com.thedeanda.lorem.Lorem;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs a test of streaming through Spark from GPUdb to GPUdb.
 * 
 * The example uses Spark to receive a data feed from a GPUdb table monitor as
 * the streaming data source.  A GPUdb table monitor provides a stream of new
 * data inserted into the source table as a ZMQ queue.  Spark will subscribe to
 * the queue, receive data, map the incoming data into the format of the target
 * GPUdb table, and write mapped records to the target GPUdb via Spark stream.
 * 
 * @author dkatz
 */
public final class StreamExample implements Serializable
{
	private static final long serialVersionUID = 3245471188522114052L;

	private static final Logger log = LoggerFactory.getLogger(StreamExample.class);

	private static final String PROP_FILE = "example.properties";
	private static final String PROP_GPUDB_HOST = "gpudb.host";
	private static final String PROP_GPUDB_QUERY_PORT = "gpudb.port.query";
	private static final String PROP_GPUDB_STREAM_PORT = "gpudb.port.stream";
	private static final String PROP_GPUDB_THREADS = "gpudb.threads";
	private static final String PROP_SPARK_CORES_MAX = "spark.cores.max";
	private static final String PROP_SPARK_EXECUTOR_MEMORY = "spark.executor.memory";
	
	private static final int INITIALIZATION_DELAY = 30;
	private static final int NEW_DATA_INTERVAL_SECS = 10;
	private static final int STREAM_POLL_INTERVAL_SECS = NEW_DATA_INTERVAL_SECS;
	private static final int INSERT_BATCH_SIZE = 10;

	private String sparkAppName;
	private String gpudbHost;
	private int gpudbPort;
	private String gpudbUrl;
	private String gpudbStreamUrl;
	private int gpudbThreads;
	private int sparkCoresMax;
	private String sparkExecutorMemory;
	private String gpudbCollectionName;
	private String gpudbSourceTableName;
	private String gpudbTargetTableName;

	private static Type type = new Type
	(
			/** Unique ID of person */
			new Type.Column("id", Long.class, Arrays.asList(ColumnProperty.DATA, ColumnProperty.PRIMARY_KEY)),
			/** Name of person */
			new Type.Column("name", String.class, new String[0]),
			/** Date of birth of person, in milliseconds since the epoch */
			new Type.Column("birthDate", Long.class, Arrays.asList(ColumnProperty.TIMESTAMP))
	);


	/**
	 * Creates a StreamTest for streaming GPUdb data into Spark
	 */
	private StreamExample()
	{
		sparkAppName = getClass().getSimpleName();
		gpudbCollectionName = "SparkExamples";
		gpudbSourceTableName = sparkAppName + ".Source";
		gpudbTargetTableName = sparkAppName + ".Target";

	}

	private void addMorePeople() throws GPUdbException
	{
		List<GenericRecord> people = new ArrayList<>();

		// Create test records
		for (int personNum = 0; personNum < INSERT_BATCH_SIZE; personNum++)
		{
			GenericRecord person = new GenericRecord(type);
			person.put("id", (long)ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
			person.put("name", Lorem.getFirstName());
			person.put("birthDate", System.currentTimeMillis());
			people.add(person);
		}
		
		log.info("Writing <{}> records to table <{}>", people.size(), gpudbSourceTableName);
		for (GenericRecord person : people)
			log.debug("    Array Item: <{}>", person);

		GPUdb gpudb = new GPUdb(gpudbUrl, new GPUdb.Options().setThreadCount(gpudbThreads));
		BulkInserter<GenericRecord> bi = new BulkInserter<>(gpudb, gpudbSourceTableName, type, people.size(), GPUdb.options());
		bi.insert(people);
		bi.flush();
	}

	/**
	 * Loads Spark application configuration using the given file
	 * 
	 * @param propFilePath path to Spark app configuration file
	 * @throws IOException if properties file fails to load
	 */
	private void loadProperties(String propFilePath) throws IOException
	{
		try (InputStream propStream = getClass().getClassLoader().getResourceAsStream(propFilePath))
		{
			Properties props = new Properties();
			props.load(propStream);
			log.info("Loaded properties file <{}>", propFilePath);

			gpudbHost = props.getProperty(PROP_GPUDB_HOST);
			gpudbPort = Integer.parseInt(props.getProperty(PROP_GPUDB_QUERY_PORT));
			gpudbUrl = "http://" + gpudbHost + ":" + Integer.parseInt(props.getProperty(PROP_GPUDB_QUERY_PORT));
			gpudbStreamUrl = "tcp://" + gpudbHost + ":" + Integer.parseInt(props.getProperty(PROP_GPUDB_STREAM_PORT));
			gpudbThreads = Integer.parseInt(props.getProperty(PROP_GPUDB_THREADS));
			sparkCoresMax = Integer.parseInt(props.getProperty(PROP_SPARK_CORES_MAX));
			sparkExecutorMemory = props.getProperty(PROP_SPARK_EXECUTOR_MEMORY);

			log.info("Using GPUdb: <{}> <{}> <{}>", gpudbUrl, gpudbSourceTableName, gpudbTargetTableName);
		}
	}
	
	/**
	 * Connects to the Spark service and returns the context for issuing tasks
	 * 
	 * @return a connected Spark context for running tasks
	 */
	private JavaSparkContext connectSpark()
	{
		// Setting insert size to "1" to avoid non-flushing due to
		//   low-throughput streaming
		SparkConf conf = new SparkConf()
			.setAppName(sparkAppName)
			.set("spark.cores.max", String.valueOf(sparkCoresMax))
			.set("spark.executor.memory", sparkExecutorMemory)
			.set(GPUdbWriter.PROP_GPUDB_HOST,  gpudbHost)
			.set(GPUdbWriter.PROP_GPUDB_PORT, String.valueOf(gpudbPort))
			.set(GPUdbWriter.PROP_GPUDB_THREADS, String.valueOf(gpudbThreads))
			.set(GPUdbWriter.PROP_GPUDB_TABLE_NAME, gpudbTargetTableName)
			.set(GPUdbWriter.PROP_GPUDB_INSERT_SIZE, "1");

		JavaSparkContext sc = new JavaSparkContext(conf);

		return sc;
    }

	/**
	 * Launches a background process that will supply the streaming data source
	 * table with new records on the defined interval.  New records added by
	 * this method will be queued by the table monitor monitoring the source
	 * table and be made available to the Spark stream receiving monitored data.
	 */
	private void launchAdder()
	{
		new Thread()
		{
			@Override
			public void run()
			{
				try
				{
					// Wait for the GPUdbReceiver to be started up
					Thread.sleep(INITIALIZATION_DELAY * 1000);

					while (true)
					{
						// Break before entering new records
						Thread.sleep(NEW_DATA_INTERVAL_SECS * 1000);

						// Add records to process
						addMorePeople();
					}
				}
				catch (InterruptedException e)
				{
					log.error("Background data-injecting thread interrupted");
				}
				catch (GPUdbException ge)
				{
					log.error("Error adding records to source table <" + gpudbSourceTableName + ">", ge);
				}
			}
		}.start();
	}

	/**
	 * Establishes a Spark stream from a GPUdb table monitor and writes streamed
	 * records to GPUdb
	 * 
	 * @throws GPUdbException if an error occurs accessing GPUdb
	 */
	private void runTest() throws GPUdbException
	{
		// Create source & destination tables in GPUdb for streaming processing
		GPUdbUtil.createTable(gpudbUrl, gpudbCollectionName, gpudbSourceTableName, type);
		GPUdbUtil.createTable(gpudbUrl, gpudbCollectionName, gpudbTargetTableName, type);

		// Launch background process for adding records to source table for
		//   table monitor to queue to the data stream
		launchAdder();

		try (JavaSparkContext sc = connectSpark())
		{
			try (JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(STREAM_POLL_INTERVAL_SECS)))
			{
				GPUdbReceiver receiver = new GPUdbReceiver(gpudbUrl, gpudbStreamUrl, gpudbSourceTableName);

				JavaReceiverInputDStream<AvroWrapper> inStream = ssc.receiverStream(receiver);

				// Must map AvroWrapper returned by GPUdb table monitor into a
				//   map of key/value pairs, which GPUdbWriter requires for
				//   writing
				JavaDStream<Map<String,Object>> outStream = inStream.map(new PersonMapFunction());

				// Perform streaming write of inbound records
				final GPUdbWriter writer = new GPUdbWriter(sc.getConf());
				writer.write(outStream);

				inStream.print();

				ssc.start();	   
				ssc.awaitTermination();
			}
		}
	}
	
	/**
	 * Concrete implementation of the map function, converting incoming wrapped
	 * data objects into maps of key/value pairs, for later insertion into the
	 * database.
	 * 
	 * @author ksutton
	 */
	private static class PersonMapFunction implements Function<AvroWrapper, Map<String,Object>>
	{
		private static final long serialVersionUID = 9139870011119019213L;

		/**
		 * Convert an Avro-wrapped record from the Spark DStream to a key/value
		 * pair map
		 * 
		 * @param t Avro-wrapped record from DStream
		 * @return the original record converted into a map of key/value pairs
		 */
		@Override
		public Map<String,Object> call(AvroWrapper t) throws Exception
		{
			Map<String,Object> record = new HashMap<String,Object>();
			org.apache.avro.generic.GenericRecord inRecord = t.getGenericRecord();

			for (Type.Column column : type.getColumns())
				record.put(column.getName(), inRecord.get(column.getName()));

			return record;
	   }
	}

	/**
	 * Launch GPUdb -> Spark DStream -> GPUdb test
	 * 
	 * @param args N/A
	 */
	public static void main(String[] args)
	{
		StreamExample example = new StreamExample();

		try
		{
			example.loadProperties(PROP_FILE);
		}
		catch (Exception e)
		{
			log.error("Failed to load properties file <{}>", PROP_FILE, e);
			System.exit(-1);
		}

		try
		{
			example.runTest();
		}
		catch (Exception e)
		{
			log.error("Problem streaming data between GPUdb and Spark", e);
			System.exit(-2);
		}
	}	
}
