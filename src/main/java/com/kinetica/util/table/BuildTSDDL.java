package com.kinetica.util.table;

/**
 * Created by sunilemanjee on 9/26/17.
 */
public class BuildTSDDL {


    public static String buildDDL(String columnName){

            return columnName+"  TYPE_TIMESTAMP";


    }

}
