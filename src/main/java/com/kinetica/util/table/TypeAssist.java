package com.kinetica.util.table;

import com.gpudb.protocol.CreateTypeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sunilemanjee on 9/22/17.
 */
public class TypeAssist {
    private static final Logger log = LoggerFactory.getLogger(TypeAssist.class);



    public static String getIntType(int i){
        if(i <= 127)
            return CreateTypeRequest.Properties.INT8;
        else if(i <= 326767)
            return CreateTypeRequest.Properties.INT16;
        else
            return "integer";

    }


    public static String getLongType(){
        return "long";

    }

    public static String getTimestampType(){
        return "long";

    }

    public static String getDoubleType(){
        return "double";
    }

    public static String getStringType(int slen){
        //int slen = s.length();

        if(slen <=1)
            return CreateTypeRequest.Properties.CHAR1;
        else if(slen<=2)
            return CreateTypeRequest.Properties.CHAR2;
        else if(slen<=4)
            return CreateTypeRequest.Properties.CHAR4;
        else if(slen<=8)
            return CreateTypeRequest.Properties.CHAR8;
        else if(slen<=16)
            return CreateTypeRequest.Properties.CHAR16;
        else if(slen<=32)
            return CreateTypeRequest.Properties.CHAR32;
        else if(slen<=64)
            return CreateTypeRequest.Properties.CHAR64;
        else if(slen<=128)
            return CreateTypeRequest.Properties.CHAR128;
        else if(slen<=256)
            return CreateTypeRequest.Properties.CHAR256;
        else
            return "string";
    }

    public static String getBytesType(){
        return "bytes";
    }
}
