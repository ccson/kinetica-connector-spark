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
public class TypeStringProcessor {
    private static final Logger log = LoggerFactory.getLogger(TypeStringProcessor.class);


    public static Dataset getMaxStringLen(Dataset ds, final String columnName){
        Encoder<Integer> integerEncoder = Encoders.INT();

        System.out.println("getMaxStringLen on "+columnName);
        Dataset dso = ds.mapPartitions(new MapPartitionsFunction() {
            List<Integer> result = new ArrayList<>();

            @Override
            public Iterator call(Iterator input) throws Exception {
                int curMax=-1;
                while (input.hasNext()) {

                    Integer wInt = null;


                    try {
                        wInt = ((Row) input.next()).getAs(columnName).toString().length();
                    } catch(NullPointerException e){
                        wInt = 0;
                    }


                    //only add if we found large value
                    //Think of this a inner partition reduce before collect()
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

        //return dso.agg(max("Year"));

        return dso.toDF(columnName).agg(max(columnName));


    }
}
