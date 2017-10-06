package com.kinetica.spark.util;

import com.gpudb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by sunilemanjee on 9/10/17.
 */
public class KineticaBulkLoader {

    private static final Logger log = LoggerFactory.getLogger(KineticaBulkLoader.class);

    private String GPUdbConnectURL;
    private Type tableType;
    private String tablename;
    private boolean isTableReplicated;
    private String gpudbIpRegex;
    private int insertSize;
    private boolean updateOnExistingPk;
    private boolean kauth;
    private String kusername;
    private String kpassword;
    private int threads;


    public KineticaBulkLoader(LoaderParams bkp ) {
        this.GPUdbConnectURL = bkp.getGPUdbURL();
        this.tableType = bkp.getTableType();
        this.tablename = bkp.getTablename();
        this.isTableReplicated = bkp.getTableReplicated();
        this.gpudbIpRegex = bkp.getGpudbIpRegex();
        this.insertSize = bkp.getInsertSize();
        this.updateOnExistingPk = bkp.getUpdateOnExistingPk();
        this.kauth = bkp.getKauth();
        this.kusername = bkp.getKusername();
        this.kpassword = bkp.getKpassword();
        this.threads = bkp.getThreads();
    }


    public BulkInserter<GenericRecord> GetBulkInserter() {
        GPUdb gpudb=null;
        try {
            log.debug("connecting to: "+ getGPUdbConnectURL(), getGPUDBOptions());
            gpudb = new GPUdb(getGPUdbConnectURL(), getGPUDBOptions());
        } catch (Exception e) {
            e.printStackTrace();
            log.error("failure:", e);
        }


        BulkInserter bulkInserter = null;


        if(isTableReplicated()){
            log.info("Table is set to Is Replication: True");
            try {
                bulkInserter = new BulkInserter <GenericRecord>(gpudb, getTableName(), getTableType(), getInsertSize(), getUpsertOptions());
            } catch (GPUdbException e) {
                e.printStackTrace();
                log.error("failure:", e);
            }
        }
        else {
            try {
                log.info("Table is not replicated table");
                bulkInserter = new BulkInserter <GenericRecord>(gpudb, getTableName(), getTableType(), getInsertSize(), getUpsertOptions(), getWorkers(gpudb));
            } catch (GPUdbException e) {
                e.printStackTrace();
                log.error("failure:", e);
            }

        }


        BulkInserter<GenericRecord> bi = bulkInserter;

        return bi;
    }

    private Map<String, String> getUpsertOptions() {
        Map<String,String> options = new HashMap<String, String>();
        options.put("update_on_existing_pk", Boolean.toString(getUpdateOnExistingPk()));
        return options;
    }

    private BulkInserter.WorkerList getWorkers(GPUdb gpudb) {

        BulkInserter.WorkerList workers = null;

        if ((getGpudbIpRegex() != null) && !(getGpudbIpRegex().trim().equalsIgnoreCase(""))) {
            log.debug("gpudbIpRegex not null: " + getGpudbIpRegex());
            Pattern pattern = Pattern.compile(getGpudbIpRegex());
            try {
                workers = new BulkInserter.WorkerList(gpudb, pattern);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("failure:", e);
            }
        } else {
            try {
                workers = new BulkInserter.WorkerList(gpudb);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("failure:", e);
            }
        }

        log.debug("Number of workers: " + workers.size());
        for (Iterator<URL> iter = workers.iterator(); iter.hasNext(); ) {
            log.debug("GPUdb BulkInserter worker: " + iter.next());
        }


        return workers;
    }


    /**
     * WritenBy - sunman
     * 9/2/2018
     * @return GPUdbBase.Options
     */
    private GPUdbBase.Options getGPUDBOptions() {
        GPUdbBase.Options opts = new GPUdb.Options();
        log.debug("Authentication enabled: " + getKauth());
        if (getKauth()) {
            log.debug("Setting username and password");
            opts.setUsername(getKusername().trim());
            opts.setPassword(getKpassword().trim());
        }
        opts.setThreadCount(getThreads());
        return opts;
    }



    public Type getTableType() {
        return tableType;
    }

    public String getTableName() {
        return tablename;
    }

    public boolean isTableReplicated() {
        return isTableReplicated;
    }

    public String getGpudbIpRegex() {
        return gpudbIpRegex;
    }

    public int getInsertSize() {
        return insertSize;
    }

    public boolean getUpdateOnExistingPk() {
        return updateOnExistingPk;
    }

    public boolean getKauth() {
        return kauth;
    }

    public String getKusername() {
        return kusername;
    }

    public String getKpassword() {
        return kpassword;
    }

    public int getThreads() {
        return threads;
    }

    public String getGPUdbConnectURL() {
        return GPUdbConnectURL;
    }
}
