package com.kinetica.spark.util;


import com.kinetica.util.properties.KineticaPropertyReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

import com.kinetica.util.KineticaConfiguration;




/**
 * Class using from spark to ingestion into Kinetica
 * using spark DataSet.
 *
 * This class uses property file or LoaderParams object to fetch
 * required parameters for ingestion
 * Created by sunilemanjee on 8/31/17.
 */
public class SparkKineticaLoader {

    private static final Logger log = LoggerFactory.getLogger(SparkKineticaLoader.class);

    private static final String PROP_FILE = "kinetica.properties";

    private static SparkContext sc1 = null;

    private static SparkSession sparkSession = null;

    /**
     * Constructor.  Will load properties from kinetica.properties
     */
    public SparkKineticaLoader() {
        try {
            loadPropertyFileFromClassPath();
        }
		catch (Exception e)
            {
                log.error("Failed to load properties file <{}>", PROP_FILE, e);
                System.exit(-1);
            }

    }




    /**
     * Creates SparkConf using properties from properties file found in class path
     * Propertyfile must be named kinetica.properties
     */
    public static void connectSpark() {
        try {
            SparkKineticaLoader.loadPropertyFileFromClassPath();
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Unable to load property file", e);
            System.exit(-2);
        }
        SparkConf conf = new SparkConf()
                .setAppName(KineticaConfiguration.PROP_SPARK_APP_NAME);

        //sc1 = SparkContext.getOrCreate(conf);
       // sc = JavaSparkContext.fromSparkContext(sc1);



       sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        //sc1 = spark.sparkContext();

    }



    /**
     * Connects to sparks using provided SparkContext and propertyFile
     * @param iSC SparkContext
     * @param propertyFile Full path to property file
     */
    public static void connectSpark(SparkSession iSC,String propertyFile ) {
        try {
            SparkKineticaLoader.loadPropertyFile(propertyFile);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Unable to load property file", e);
            System.exit(-2);
        }



         sparkSession = iSC;

    }

    /**
     * Returns SparkContext
     * @return SparkContext
     */
    public  static SparkContext getSparkContext(){
        return sc1;
    }

    /**
     * Creates SparkConf using properties from properties file
     * @param propertyFile kinetica property file
     */
    public static void connectSpark(String propertyFile) {

        try {
            SparkKineticaLoader.loadPropertyFile(propertyFile);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Unable to load property file", e);
            System.exit(-2);
        }

        SparkConf conf = new SparkConf()
                .setAppName(KineticaConfiguration.PROP_SPARK_APP_NAME);



        sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

    }

    /**
     *
     * Returns Spark Context, if SparkContext is null will create one
     * @return JavaSparkContext
     */
    public static SparkSession getSparkSession() {
        if (sparkSession == null){
            connectSpark();
        }
        return sparkSession;
    }

    /**
     * Bring your own Spark Context
     * @param Jsc JavaSparkContext
     */
    public static void setSparkSession(SparkSession Jsc) {

        sparkSession = Jsc;
    }


    /**
     * Loads Spark application configuration from class path kinetica.properties
     *
     * @throws IOException if properties file fails to load
     */
    private static void loadPropertyFileFromClassPath() throws IOException
    {
        //try (InputStream propStream = getClass().getClassLoader().getResourceAsStream(propFilePath))
        try (InputStream propStream = SparkKineticaLoader.class.getClassLoader().getResourceAsStream(PROP_FILE))
        {
            //Properties props = new Properties();
            //props.load(propStream);
            KineticaPropertyReader.init(propStream);
            log.info("Loaded properties file <{}>", PROP_FILE);


            log.info("Using GPUdb: <" + KineticaConfiguration.GPUDB_CONNECT_STRING + "," + KineticaConfiguration.TABLE_NAME + ">");
        }
    }

    /**
     * Loads property files - Requires full path
     * @param propertyFile
     * @throws IOException
     */
    private static void loadPropertyFile(String propertyFile) throws IOException{
        KineticaPropertyReader.init(propertyFile);

    }

    /**
     * Stop sparkcontext
     */
    public static void stopSC(){
        sparkSession.stop();
    }




    /**
     * Write DataSet to Kinetica using supplied parameters from LoaderParams object and spark session
     * @param df DataSet
     * @param lp LoaderParam object
     * @param mapToSchema Map DataSet schema (column names) to kinetica table names.
     *                    true:  will map DataSet column names to Kinetica
     *                    table column names
     *                    false: will map chronological order in DataSet to chronological
     *                    order to Kinetica Table columns
     */
    public static void KineticaWriter(Dataset df, LoaderParams lp, boolean mapToSchema){


        log.debug("Get Kinetica Table Type");
        KineticaSparkDFHelper.setType(lp);

        log.debug("Set LoaderParms Table Type");
        lp.setTableType(KineticaSparkDFHelper.getType());

        log.debug("Set spark column list");
        KineticaSparkDFHelper.setClist();


        log.debug("Set DataFrame");
        KineticaSparkDFHelper.setDF(df);


        if (lp.getTableReplicated()){
            log.info("Table is replicated");
        }
        else {
            log.info("Table is not replicated");
        }


        log.debug("Map and Write to Kinetica");
        KineticaSparkDFHelper.KineticaMapWriter(lp, mapToSchema);



    }



    /**
     * Note - This is experimental feature.  Not intended to production use.
     * Loads delimited file from local disk/mount or  HDFS into spark Dataframe
     * which will load into Kinetica for MPP ingest.
     * First line of file is expected to be header
     * to infer schema.   Fields names must match kinetica table field names
     * @param iSC SparkSession
     * @param lp LoaderParams object
     * @param filePath Full Path to local file or HDFS including namenode
     * @param delimiter file delimiter
     * @param headerIncluded
     *                    true:  will map DataSet column names to Kinetica
     *                    table column names
     *                    false: will map chronological order in DataSet to chronological
     *                    order to Kinetica Table columns
     */
    public static void LoadFromFile(SparkSession iSC, LoaderParams lp, String filePath, String delimiter, boolean headerIncluded){
        setSparkSession(iSC);
        KineticaSparkDFHelper.DataFrameFromFile(filePath, delimiter, headerIncluded, lp);
    }


    /**
     * Note - This is experimental feature.  Not intended to production use.
     * Method used to create DataSet from file types orc, parquet, and json
     * @param iSC SparkSession
     * @param lp LoaderParams object
     * @param filePath full file path either local or hdfs
     * @param fileType orc, parquet or json
     */
    public static void LoadFromFile(SparkSession iSC, LoaderParams lp, String filePath, String fileType){
        setSparkSession(iSC);
        KineticaSparkDFHelper.DataFrameFromFile(filePath, fileType, lp);
    }

}