package com.kinetica.util.table;

/**
 * Created by sunilemanjee on 9/26/17.
 */
public class BuildByteDDL {


    /**
     * TODO byte not support yet in 6.0.1
     * @param columnName
     * @return
     */
    public static String buildDDL(String columnName){

            return columnName+" BYTE";


    }

}
