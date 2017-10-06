package com.kinetica.util.table;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.max;

/**
 * Created by sunilemanjee on 9/23/17.
 */
public class TypeIntProcessor {
    private static final Logger log = LoggerFactory.getLogger(TypeIntProcessor.class);


    public static Dataset getMaxInt(Dataset ds, final String columnName){
        Encoder<Integer> integerEncoder = Encoders.INT();

        log.debug("getMaxInt on "+columnName);
        Dataset dso = ds.mapPartitions(new MapPartitionsFunction() {
            List<Integer> result = new ArrayList<>();

            @Override
            public Iterator call(Iterator input) throws Exception {
                int curMax=-1;
                while (input.hasNext()) {


                    Integer wInt = ((Row) input.next()).getAs(columnName);

                    //only add if we found large value
                    //Think of this a reduce before the partition reduce
                    log.debug("wInt "+ wInt.intValue());
                    log.debug("curMax"+ curMax);

                    log.debug("Checking max int");
                    if (wInt.intValue()>curMax) {
                        result.add(wInt);
                        curMax = wInt.intValue();
                    }
                }
                return result.iterator();

            }
        }, integerEncoder);


        return dso.toDF(columnName).agg(max(columnName));


    }
}
