package com.kinetica.util.table;

import com.kinetica.spark.util.JsonTypeBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sunilemanjee on 9/23/17.
 */
public class ColumnProcessor {
    private static final Logger log = LoggerFactory.getLogger(ColumnProcessor.class);

    public static void processNumeric(DataType dt, String columnName, boolean nullable) {
        if(dt instanceof ByteType){
            KineticaQueryBuilder.buildNumeric(columnName, "int8", nullable);
        }
        else if(dt instanceof ShortType){
            KineticaQueryBuilder.buildNumeric(columnName, "int16", nullable);
        }
        else if(dt instanceof IntegerType){
            KineticaQueryBuilder.buildNumeric(columnName, "integer", nullable);
        }
        else if(dt instanceof LongType){
            KineticaQueryBuilder.buildNumeric(columnName, "long", nullable);
        }
        else if(dt instanceof FloatType){
            KineticaQueryBuilder.buildNumeric(columnName, "float", nullable);
        }
        else if(dt instanceof DoubleType){
            KineticaQueryBuilder.buildNumeric(columnName, "double", nullable);
        }
        else if(dt instanceof DecimalType){
            KineticaQueryBuilder.buildNumeric(columnName, "decimal", nullable);
        }
        else {
            KineticaQueryBuilder.buildNumeric(columnName, "double", nullable);
        }

    }



    public static void processString(Dataset ds, String columnName, boolean nullable){

        Dataset maxIntDs;
        maxIntDs = TypeStringProcessor.getMaxStringLen(ds, columnName);

        Row r = (Row) maxIntDs.first();
        int maxInt  = r.getInt(0);

        KineticaQueryBuilder.buildString(columnName, maxInt, nullable);



    }

    public static void processTS(Dataset ds, String columnName, boolean nullable){

        KineticaQueryBuilder.buildTS(columnName, nullable);

    }

    public static void processDate(Dataset ds, String columnName, boolean nullable){

        KineticaQueryBuilder.buildDate(columnName, nullable);

    }

    public static void processBoolean(Dataset ds, String columnName, boolean nullable){

        KineticaQueryBuilder.buildBoolean(columnName, nullable);

    }

}
