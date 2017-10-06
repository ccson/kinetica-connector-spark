package com.kinetica.spark.util;

import org.json.JSONObject;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sunilemanjee on 9/22/17.
 */
public class JsonTypeBuilder {

    private static final Logger log = LoggerFactory.getLogger(JsonTypeBuilder.class);




    private static JSONObject newType = null;
    private static JSONArray fieldArray = null;


    public static void initType(String tableName){

        newType = new JSONObject();
        newType.put("type", "record");
        newType.put("name", tableName);

        fieldArray = new JSONArray();

    }


    public static void addFieldToType(String columnName, String columnType){

        JSONObject column = new JSONObject();
        column.put("name", columnName);
        column.put("type", columnType);

        fieldArray.put(column);



    }

    public static void closeType(){
        System.out.println("Closing type");

        newType.put("fields", fieldArray);

        log.info(newType.toString());
        System.out.println(newType.toString());
    }
}
