package com.kinetica.util;

import com.kinetica.spark.util.KineticaBulkLoader;
import com.kinetica.util.properties.KineticaPropertyReader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class KineticaConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KineticaConfiguration.class);


    private static Properties kineticaProperties =
            KineticaPropertyReader.getInstance();

    //First the Mandatory values
    public static final String TABLE_NAME =
            kineticaProperties.getProperty("KINETICA.TABLE.TABLENAME").trim();


    public static final String GPUDB_CONNECT_STRING =
            kineticaProperties.getProperty("GPUDB.CONNECT.STRING").trim();


    public static final Integer PROP_GPUDB_THREADS =
            Integer.parseInt(kineticaProperties.getProperty("gpudb.threads").trim());


    public static final String PROP_SPARK_APP_NAME =
            kineticaProperties.getProperty("spark.app.name").trim();


    // Now the Optional Values
    public static final String TRUNCATE_TABLE =
            kineticaProperties.getProperty("KINETICA.TRUNCATE.TABLE").trim();


    public static final String COLLECTION_NAME =
            kineticaProperties.getProperty("KINETICA.TABLE.COLLECTION.NAME");

    public static final boolean IS_REPLICATED =
            Boolean.parseBoolean(kineticaProperties.
                    getProperty("KINETICA.TABLE.REPLICATED").trim().toLowerCase());


    public static final boolean DELETE_TABLE_IF_EXISTS =
            Boolean.parseBoolean(kineticaProperties.
                    getProperty("KINETICA.TABLE.DELETE.IF.EXISTS").trim().toLowerCase());



    public static final boolean UPDATE_ON_EXISTING_PK =
            Boolean.parseBoolean(kineticaProperties.getProperty(
                    "KINETICA.TABLE.UPDATE.ON.EXISTING.PK").trim().toLowerCase());



    public static final int BULKINGESTOR_BATCH_SIZE =
            new Integer(kineticaProperties.getProperty(
                    "BULKINGESTOR.BATCH.SIZE").trim()).intValue();

    public static final String IP_REGEX =
            kineticaProperties.getProperty("GPUDB.IP.REGEX").trim();

    public static final boolean ENABLE_AUTHENTICATION =
            Boolean.parseBoolean(kineticaProperties.getProperty("KINETICA.ENABLE.AUTHENTICATION").trim().toLowerCase());

    public static final String USERNAME =
            kineticaProperties.getProperty("KINETICA.USERNAME").trim();

    public static final String PASSWORD =
            kineticaProperties.getProperty("KINETICA.PASSWORD").trim();



    public static void validateMandatoryProperties() {
        if (StringUtils.isEmpty(KineticaConfiguration.GPUDB_CONNECT_STRING)) {
            //throw new KineticaException("Property GPUDB.CONNECT_STRING can't be null");
        }
        if (StringUtils.isEmpty(KineticaConfiguration.TABLE_NAME)) {
            //throw new KineticaException("Property KINETICA.TABLE.TABLENAME can't be null");
        }
    }

    public static void validateOptionalProperties() {
        if (StringUtils.isEmpty(KineticaConfiguration.TRUNCATE_TABLE)) {
            //throw new KineticaException("Property KINETICA.TRUNCATE_TABLE can't be null. Valid values are true/false.");
        }
        if (StringUtils.isEmpty(new Boolean(KineticaConfiguration.IS_REPLICATED).toString())) {
            //throw new KineticaException("Property KINETICA.TABLE.REPLICATED can't be null");
        }

        if (KineticaConfiguration.ENABLE_AUTHENTICATION) {
            if (StringUtils.isEmpty(KineticaConfiguration.USERNAME)) {
                //throw new KineticaException("Authentication is enabled. Property KineticaConfiguration.USERNAME can't be null.");
            }
            if (StringUtils.isEmpty(KineticaConfiguration.PASSWORD)) {
                //throw new KineticaException("Authentication is enabled. Property KineticaConfiguration.PASSWORD can't be null.");
            }
        }

    }
}