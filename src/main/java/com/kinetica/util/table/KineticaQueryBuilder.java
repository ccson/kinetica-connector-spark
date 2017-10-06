package com.kinetica.util.table;

import java.io.Serializable;

/**
 * Created by sunilemanjee on 9/25/17.
 */
public class KineticaQueryBuilder implements Serializable {

    private static StringBuffer createTableDDL = null;
    private static boolean firstColumn=true;

    public static void init(String tableName){
        createTableDDL = new StringBuffer().append("CREATE OR REPLACE TABLE "+tableName+" (");
    }

    public static void initReplicated(String tableName){
        createTableDDL.append("CREATE OR REPLACE REPLICATED TABLE "+tableName+" (");
    }

    public static void buildNumeric(String columnName, String intType, boolean nullable){
        addToDDL(BuildNumericDDL.buildDDL(columnName, intType), nullable);
    }

    public static void buildString(String columnName, int maxStringLen, boolean nullable){
        addToDDL(BuildStringDDL.buildDDL(columnName, maxStringLen), nullable);
    }


    public static void buildTS(String columnName, boolean nullable){
        addToDDL(BuildTSDDL.buildDDL(columnName), nullable);
    }

    public static void buildDate(String columnName, boolean nullable){
        addToDDL(BuildDateDDL.buildDDL(columnName), nullable);
    }

    public static void buildBoolean(String columnName, boolean nullable){
        addToDDL(BuildBooleanDDL.buildDDL(columnName), nullable);
    }

    public static void closeDDL(){
        createTableDDL.append(" )");
        System.out.println(createTableDDL.toString());
        firstColumn = true;
    }

    private static void addToDDL(String ddlmod, boolean nullable){
        if(!nullable){
            ddlmod.concat(" NOT NULL");
        }

        if(firstColumn) {
            createTableDDL.append(" " + ddlmod);
            firstColumn = false;
        }
        else {
            createTableDDL.append(", " + ddlmod);
        }
    }

    public static String getCreateTableDDL() {
        return createTableDDL.toString();
    }
}
