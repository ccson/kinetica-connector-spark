package com.kinetica.util.table;

/**
 * Created by sunilemanjee on 9/26/17.
 */
public class BuildBooleanDDL {


    public static String buildDDL(String columnName){

            return columnName+"  DECIMAL(1,0)";


    }

}
