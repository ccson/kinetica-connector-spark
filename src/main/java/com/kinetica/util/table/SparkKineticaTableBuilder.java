package com.kinetica.util.table;
import com.kinetica.exception.KineticaException;
import com.kinetica.spark.util.LoaderParams;
import com.kinetica.util.JDBCConnectionUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sunilemanjee on 9/21/17.
 *
 * Converst spark datatype to kinetica datatype
 * http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types
 */
public class SparkKineticaTableBuilder {
    private static final Logger log = LoggerFactory.getLogger(SparkKineticaTableBuilder.class);



    public static void KineticaMapWriter(Dataset ds, LoaderParams lp) throws KineticaException{



        log.info("KineticaMapWriter");


        log.debug("Mapping Dataset columns to Kinetica");

        try {
            if (lp.getJdbcURL().trim().isEmpty())
                throw new KineticaException("Missing JDBC URL or invalid");
        }catch (Exception e) {
            log.error("JDBC url missing");
            throw new KineticaException("Missing JDBC URL or invalid");
        }




        /*

        for (Tuple2 tuple2 : ds.dtypes()) {
            String columname = (String) tuple2._1();
            String columnType = (String) tuple2._2();
        }
        */


        KineticaQueryBuilder.init(lp.getTablename());
        //JsonTypeBuilder.initType(tableName);

        for (scala.collection.Iterator<StructField> sField = ds.schema().iterator(); sField.hasNext();){
            StructField sf2 = sField.next();
            DataType dt = sf2.dataType();


            System.out.println("Working on field: "+ sf2.name());
            

            if(dt instanceof NumericType){
                System.out.println("Found numeric type");
                ColumnProcessor.processNumeric(dt, sf2.name(), sf2.nullable());
            }

            else if(dt instanceof StringType){
                ColumnProcessor.processString(ds, sf2.name(), sf2.nullable());
            }
            else if(dt instanceof TimestampType){
                ColumnProcessor.processTS(ds,sf2.name(), sf2.nullable());
            }
            else if(dt instanceof DateType){
                ColumnProcessor.processTS(ds,sf2.name(), sf2.nullable());
            }
            else if(dt instanceof BooleanType){
                ColumnProcessor.processBoolean(ds,sf2.name(), sf2.nullable());
            }

        }
        KineticaQueryBuilder.closeDDL();
        //JsonTypeBuilder.closeType();

        JDBCConnectionUtils.Init(lp.getJdbcURL());
        JDBCConnectionUtils.executeSQL(KineticaQueryBuilder.getCreateTableDDL());



    }



}
