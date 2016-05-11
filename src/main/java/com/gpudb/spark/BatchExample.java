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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs tests of the non-streaming Spark & GPUdb APIs.
 * 
 * The test is primed by creating three PersonRecord objects, adding them to a
 * SparkContext, and having Spark write them to GPUdb.
 * 
 * The first check uses the GPUdb Spark connector's readTable to retrieve the
 * records written in the priming phase and verifying they match the initial
 * three expected to be inserted.  An intersection between the resulting RDD
 * and the initial one used to insert the records is performed to ensure all
 * records were found.
 * 
 * The second check involves passing the same three PersonRecord objects to
 * Spark and having Spark query GPUdb to ensure the records were written to
 * GPUdb successfully.  A count is performed on the result set to ensure all
 * records were found.
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
	private static final String PROP_SPARK_CORES_MAX = "spark.cores.max";

	private String gpudbHost;
	private int gpudbPort;
	private String gpudbUrl;
	private int gpudbThreads;
	private String gpudbTableName;
	private String sparkAppName;
	private int sparkCoresMax;
	
	List<PersonRecord> people = new ArrayList<>();


	BatchExample()
	{
		sparkAppName = getClass().getSimpleName();
		gpudbTableName = "Spark." + sparkAppName;

		// Create test records
		people.add(new PersonRecord(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE), "John", System.currentTimeMillis()));
		people.add(new PersonRecord(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE), "Anna", System.currentTimeMillis()));
		people.add(new PersonRecord(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE), "Andrew", System.currentTimeMillis()));
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
			sparkCoresMax = Integer.parseInt(props.getProperty(PROP_SPARK_CORES_MAX));

			log.info("Using GPUdb: <" + gpudbUrl + "," + gpudbTableName + ">");
		}
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
			.set(GPUdbWriter.PROP_GPUDB_TABLE_NAME,  gpudbTableName);

		JavaSparkContext sc = new JavaSparkContext(conf);

		return sc;
	}

	private void runTest()
	{
		long numFound = 0;

		try (JavaSparkContext sc = connectSpark())
		{
			// Create table for test records in GPUdb
			GPUdbUtil.createTable(gpudbUrl, gpudbTableName, PersonRecord.class);

			// Add records to process
			JavaRDD<PersonRecord> rdd = sc.parallelize(people);

			// Write records through Spark to GPUdb
			final GPUdbWriter<PersonRecord> writer = new GPUdbWriter<PersonRecord>(sc.getConf());
			writer.write(rdd);

			// Read records from GPUdb via Spark connector
			final GPUdbReader<PersonRecord> reader = new GPUdbReader<PersonRecord>(sc.getConf());
			numFound = reader.readTable(PersonRecord.class, null, sc).intersection(rdd).count();

			if (people.size() == numFound)
				log.info("*****  All PersonRecord objects found  *****", numFound);
			else
				log.error("*****  Not all PersonRecord objects found <{}/{}>  *****", numFound, people.size());

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
