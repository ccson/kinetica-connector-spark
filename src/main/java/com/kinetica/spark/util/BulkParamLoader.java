package com.kinetica.spark.util;

import com.gpudb.Type;
import com.kinetica.util.KineticaConfiguration;

/**
 * Created by sunilemanjee on 9/10/17.
 */
public class BulkParamLoader {
    private LoaderParams bkp;


    public LoaderParams getBulkLoaderParams(Type tableType){
        bkp = new LoaderParams();
        bkp.setGPUdbURL(KineticaConfiguration.GPUDB_CONNECT_STRING);
        bkp.setTableType(tableType);
        bkp.setTablename(KineticaConfiguration.TABLE_NAME);
        bkp.setTableReplicated(KineticaConfiguration.IS_REPLICATED);
        bkp.setGpudbIpRegex(KineticaConfiguration.IP_REGEX.trim());
        bkp.setInsertSize(KineticaConfiguration.BULKINGESTOR_BATCH_SIZE);
        bkp.setUpdateOnExistingPk(KineticaConfiguration.UPDATE_ON_EXISTING_PK);
        bkp.setKauth(KineticaConfiguration.ENABLE_AUTHENTICATION);
        bkp.setKusername(KineticaConfiguration.USERNAME);
        bkp.setKpassword(KineticaConfiguration.PASSWORD);
        bkp.setThreads(KineticaConfiguration.PROP_GPUDB_THREADS);

        return bkp;

    }
}
