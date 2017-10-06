package com.kinetica.spark.util;

import com.gpudb.*;
import com.kinetica.util.GPUConnectionManager;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.BooleanType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import com.kinetica.util.KineticaConfiguration;


/**
 * Assisting with mapping over partition
 * Created by sunilemanjee on 9/4/17.
 */
public class KineticaSparkDFHelper {

    private static final Logger log = LoggerFactory.getLogger(KineticaSparkDFHelper.class);


    static Dataset df;
    static Type type;

    static ArrayList<Column> clist = new ArrayList<>();

    public static JavaSparkContext getSc() {
        return sc;
    }

    public static void setSc(JavaSparkContext sc) {
        KineticaSparkDFHelper.sc = sc;
    }

    private static JavaSparkContext sc;

    public static void setType(){
        try {
            type = Type.fromTable(GPUConnectionManager.getInstance().getGPUdb(), KineticaConfiguration.TABLE_NAME);
        } catch (GPUdbException e) {
            e.printStackTrace();
        }

    }

    public static Type getType() {
        return type;
    }

    public static void setType(LoaderParams lp){
        try {
            GPUdb gpudb = new GPUdb(lp.getGPUdbURL(), getGPUDBOptions(lp));
            type = Type.fromTable(gpudb, lp.getTablename());
        } catch (GPUdbException e) {
            e.printStackTrace();
        }

    }

    private static GPUdbBase.Options getGPUDBOptions(LoaderParams lp) {
        GPUdbBase.Options opts = new GPUdb.Options();
        boolean enableAuthentication = new Boolean(lp.getKauth()).booleanValue();
        log.debug("Authentication enabled: " + enableAuthentication);
        if (enableAuthentication) {
            log.debug("Setting username and password");
            opts.setUsername(lp.getKusername().trim());
            opts.setPassword(lp.getKpassword().trim());
        }
        opts.setThreadCount(lp.getThreads());
        return opts;
    }

    public static void setClist(){
        for (Type.Column column : type.getColumns()){
            clist.add(new Column(column.getName()));
        }
    }

    public static ArrayList<Column> getClist(){
        return clist;
    }


    public static void setDF(Dataset df2){
        df = df2;

    }

    public static Dataset getDF(){
        return df;
    }



    /**
     * Maps over dataframe using either matching columns or chronological ordering
     * @param lp LoaderParams
     * @param mapToSchema true or false if map over columns names. false will map over chronological order
     */
    public static void KineticaMapWriter(LoaderParams lp, boolean mapToSchema) {

        log.info("KineticaMapWriter");

        final Type typef = type;


        final LoaderParams bkp = lp;


        if(mapToSchema) {
            log.debug("Mapping Dataset columns to Kinetica");


            df.foreachPartition(new ForeachPartitionFunction<Row>() {
                public void call(Iterator<Row> t) throws Exception {
                    KineticaBulkLoader kbl = new KineticaBulkLoader(bkp);
                    BulkInserter<GenericRecord> bi = kbl.GetBulkInserter();
                    //used to determine if we find matching column from dataset to table
                    //if found we will insert record.  if no columns match we will terminate
                    boolean isARecord = false;
                    while (t.hasNext()) {
                        Row row = t.next();
                        GenericRecord genericRecord = new GenericRecord(typef);
                        for (Type.Column column : typef.getColumns()) {

                                try {
                                    Object rtemp = row.getAs(column.getName());


                                    //if (column.getType().isInstance(rtemp) && rtemp != null) {
                                    if(rtemp != null) {
                                        log.debug("object not null");
                                        if (rtemp instanceof Timestamp) {
                                            log.debug("Timestamp instance");
                                            genericRecord.put(column.getName(), Timestamp.class.cast(rtemp).getTime());
                                            isARecord=true;
                                        }
                                        else if (rtemp instanceof Boolean) {
                                            log.debug("Boolean instance");
                                            if(Boolean.class.cast(rtemp).booleanValue()) {
                                                log.debug("Cast to 1");
                                                genericRecord.put(column.getName(), 1);
                                                isARecord=true;
                                            }
                                            else {
                                                log.debug("Cast to 0");
                                                genericRecord.put(column.getName(), 0);
                                                isARecord=true;
                                            }
                                        }
                                        else if (column.getType().isInstance(rtemp)) {
                                            log.debug("other instance");
                                            genericRecord.put(column.getName(), column.getType().cast(rtemp));
                                            isARecord=true;
                                        }
                                    }
                                }catch (IllegalArgumentException e){
                                    log.debug("Found non-matching column DS.column --> KineticaTable.column, moving on", e);
                                }

                        }
                        if(isARecord) {
                            bi.insert(genericRecord);
                            //reset for next record
                            isARecord=false;
                        }

                    }
                    try {
                        bi.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Flush error", e);
                    }
                }
            });
        }
        else{
            log.debug("Mapping chronological column order Kinetica table");
            df.foreachPartition(new ForeachPartitionFunction<Row>() {
                public void call(Iterator<Row> t) throws Exception {
                    KineticaBulkLoader kbl = new KineticaBulkLoader(bkp);
                    BulkInserter<GenericRecord> bi = kbl.GetBulkInserter();
                    while (t.hasNext()) {
                        Row row = t.next();
                        GenericRecord genericRecord = new GenericRecord(typef);
                        int i = 0;
                        for (Type.Column column : typef.getColumns()) {
                            Object rtemp = row.get(i++);

                            //if (column.getType().isInstance(rtemp) && rtemp != null) {
                            if (rtemp != null) {

                                genericRecord.put(column.getName(), column.getType().cast(rtemp));
                            }

                        }
                        i=0;
                        bi.insert(genericRecord);

                    }
                    try {
                        bi.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Flush error", e);
                    }
                }
            });

        }


    }



    /**
     *
     * @param filePath full path to local file or  hdfs file include namenode and hdfs://
     * @param delimiter file delimiter
     * @param headerIncluded does file include header to infer DataSet column names. if set to false will map to chronological column order during ingest
     */

    public static void DataFrameFromFile(String filePath, String delimiter, boolean headerIncluded, LoaderParams lp){
        //create data frame from hdfs, airline data

            SparkKineticaLoader.KineticaWriter(SparkKineticaLoader.getSparkSession().read()
                    .format("csv")
                    .option("header", Boolean.toString(headerIncluded)) //reading the headers
                    .option("inferSchema", "true")
                    .option("delimiter", delimiter)
                    .csv(filePath),
                    lp, headerIncluded);


    }


    /**
     * Note - This is experimental feature.  Not intended to production use.
     * Method used to create DataSet from file types orc, parquet, and json
     * @param filePath full file path local or hdfs
     * @param fileType orc, parquet, avro or json
     */
    public static void DataFrameFromFile(String filePath, String fileType, LoaderParams lp){
        //create data frame from hdfs, airline data


        //if orc
        if(fileType.trim().compareToIgnoreCase("orc") ==0){
            SparkKineticaLoader.KineticaWriter(SparkKineticaLoader.getSparkSession().read().orc(filePath), lp, true);

        } //if parquet
        else if (fileType.trim().compareToIgnoreCase("parquet") ==0){
            SparkKineticaLoader.KineticaWriter(SparkKineticaLoader.getSparkSession().read().parquet(filePath),lp,  true);
        } //if json
        else if(fileType.trim().compareToIgnoreCase("json") ==0) {
        SparkKineticaLoader.KineticaWriter(SparkKineticaLoader.getSparkSession().read()
                .option("multiLine", "true")
                .json(filePath), lp,  true);
        }
        else if(fileType.trim().compareToIgnoreCase("avro") ==0){
            SparkKineticaLoader.KineticaWriter(SparkKineticaLoader.getSparkSession().read().format("com.databricks.spark.avro").load(filePath), lp, true);
        }

    }

}
