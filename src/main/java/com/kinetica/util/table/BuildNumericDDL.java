package com.kinetica.util.table;

/**
 * Created by sunilemanjee on 9/26/17.
 */
public class BuildNumericDDL {


    public static String buildDDL(String columnName, String intType){

        if(intType.compareToIgnoreCase("int8")==0)
        {
            return columnName+"  DECIMAL(2,0)";

        }
        else if(intType.compareToIgnoreCase("int16")==0)
        {
            return columnName+"  DECIMAL(4,0)";

        }
        else if(intType.compareToIgnoreCase("integer")==0)
        {
            return columnName+"  INTEGER";

        }
        else if(intType.compareToIgnoreCase("long")==0)
        {
            return columnName+"  LONG";

        }
        else if(intType.compareToIgnoreCase("decimal")==0)
        {
            /**
             * ToDo need to add arguments instead of hard coding
             * 10 10
             */
            return columnName+"  DECIMAL(10,10)";

        }
        else if(intType.compareToIgnoreCase("float")==0)
        {
            return columnName+" FLOAT";

        }
        else if(intType.compareToIgnoreCase("double")==0)
        {
            return columnName+"  DOUBLE";

        }

        return null;
    }

}
