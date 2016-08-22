package com.gpudb.spark;

import com.gpudb.spark.dao.PersonRecord;
import com.gpudb.spark.input.GPUdbReader;
import com.gpudb.spark.output.GPUdbWriter;
import com.gpudb.spark.util.GPUdbUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.thedeanda.lorem.Lorem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs tests of the non-streaming Spark & GPUdb APIs.
 * 
 * The test is primed by creating three PersonRecord objects, adding them to a
 * Spark RDD, and using the GPUdb Spark connector to write the RDD to GPUdb.
 * 
 * The test then uses the GPUdb Spark connector to retrieve the records written
 * in the priming phase and verify they match the initial three expected to be
 * inserted.  An intersection between the resulting RDD and the initial one used
 * to insert the records is performed to ensure all records were found.
 * 
 * @author ksutton
 */
public class BatchExample implements Serializable
{
	private static final long serialVersionUID = -3290252846935753374L;

	private static final Logger log = LoggerFactory.getLogger(BatchExample.class);

	private static final String PROP_FILE = "example.properties";
	private static final String PROP_GPUDB_HOST = "gpudb.host";
	private static final String PROP_GPUDB_PORT = "gpudb.port.query";
	private static final String PROP_GPUDB_THREADS = "gpudb.threads";
	private static final String PROP_GPUDB_READ_SIZE = "gpudb.read.size";
	private static final String PROP_GPUDB_INSERT_SIZE = "gpudb.insert.size";
	private static final String PROP_SPARK_CORES_MAX = "spark.cores.max";
	private static final String PROP_BATCH_RECORD_COUNT = "batch.record.count";

	private String gpudbHost;
	private int gpudbPort;
	private String gpudbUrl;
	private int gpudbThreads;
	private int gpudbReadSize;
	private int gpudbInsertSize;
	private String gpudbCollectionName;
	private String gpudbTableName;
	private String sparkAppName;
	private int sparkCoresMax;
	private int batchRecordCount;
	private long exampleStartTime;
	
	List<PersonRecord> people = new ArrayList<>();


	BatchExample()
	{
		sparkAppName = getClass().getSimpleName();
		gpudbCollectionName = "SparkExamples";
		gpudbTableName = sparkAppName;
		exampleStartTime = System.currentTimeMillis();
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
			gpudbPort = Integer.parseInt(props.getProperty(PROP_GPUDB_PORT));
			gpudbUrl = "http://" + gpudbHost + ":" + gpudbPort;
			gpudbThreads = Integer.parseInt(props.getProperty(PROP_GPUDB_THREADS));
			gpudbReadSize = Integer.parseInt(props.getProperty(PROP_GPUDB_READ_SIZE));
			gpudbInsertSize = Integer.parseInt(props.getProperty(PROP_GPUDB_INSERT_SIZE));
			sparkCoresMax = Integer.parseInt(props.getProperty(PROP_SPARK_CORES_MAX));
			batchRecordCount = Integer.parseInt(props.getProperty(PROP_BATCH_RECORD_COUNT));

			log.info("Using GPUdb: <" + gpudbUrl + "," + gpudbTableName + ">");
		}
	}

	/**
	 * Generates the configured number of batch test records
	 */
	private void generateRecords()
	{
		int startID = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);

		for (int batchRecordNum = 0; batchRecordNum < batchRecordCount; batchRecordNum++)
			people.add(new PersonRecord(startID + batchRecordNum, Lorem.getFirstName(), System.currentTimeMillis()));
	}

	private JavaSparkContext connectSpark()
	{
		SparkConf conf = new SparkConf()
			.setAppName(sparkAppName)
			.set("spark.blockManager.port", "50001")
			.set("spark.broadcast.port", "50002")
			.set("spark.driver.port", "50003")
			.set("spark.executor.port", "50004")
			.set("spark.fileserver.port", "50005")
			.set("spark.cores.max", String.valueOf(sparkCoresMax))
			.set(GPUdbWriter.PROP_GPUDB_HOST,  gpudbHost)
			.set(GPUdbWriter.PROP_GPUDB_PORT, String.valueOf(gpudbPort))
			.set(GPUdbWriter.PROP_GPUDB_THREADS, String.valueOf(gpudbThreads))
			.set(GPUdbReader.PROP_GPUDB_READ_SIZE, String.valueOf(gpudbReadSize))
			.set(GPUdbWriter.PROP_GPUDB_INSERT_SIZE, String.valueOf(gpudbInsertSize))
			.set(GPUdbWriter.PROP_GPUDB_TABLE_NAME,  gpudbTableName);

		JavaSparkContext sc = new JavaSparkContext(conf);

		return sc;
	}

	private void runTest()
	{
		long numFound = 0;
		long numMatch = 0;

		try (JavaSparkContext sc = connectSpark())
		{
			// Create table for test records in GPUdb
			GPUdbUtil.createTable(gpudbUrl, gpudbCollectionName, gpudbTableName, PersonRecord.class);

			// Add records to process
			JavaRDD<PersonRecord> rdd = sc.parallelize(people);

			// Write records through Spark to GPUdb
			final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sc.getConf());
			writer.write(rdd);

			// Read records from GPUdb via Spark connector,
			//   caching the result RDD so that validation checks don't cause a
			//   re-querying for the data
			final GPUdbReader<PersonRecord> reader = new GPUdbReader<PersonRecord>(sc.getConf());
			JavaRDD<PersonRecord> rddPeopleFound = reader.readTable(PersonRecord.class, "birthDate >= " + exampleStartTime, sc).cache();
			numFound = rddPeopleFound.count();
			numMatch = rddPeopleFound.intersection(rdd).count();
			
			if (people.size() == numFound && numFound == numMatch)
				log.info("*****  All PersonRecord objects found  *****", numFound);
			else
				log.error("*****  Not all PersonRecord objects found <{}/{}> vs <{}> *****", numMatch, numFound, people.size());
			
			sc.stop();
		}
		catch (Exception ex)
		{
			log.error("Error processing records", ex);
		}
	}


	/**
	 * Run Spark/GPUdb test program
	 * 
	 * @param args not used
	 */
	public static void main(String[] args)
	{
		BatchExample example = new BatchExample();

		try
		{
			example.loadProperties(PROP_FILE);

			example.generateRecords();
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
			log.error("Problem running test", e);
			System.exit(-2);
		}
	}
}
