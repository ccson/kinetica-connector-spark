package com.kinetica.util.table;

/**
 * Created by sunilemanjee on 9/26/17.
 */
public class BuildStringDDL {


    public static String buildDDL(String columnName, int maxStringLen){

            return columnName+" VARCHAR("+maxStringLen+")";

    }

}
