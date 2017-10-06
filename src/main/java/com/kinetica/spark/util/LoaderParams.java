package com.kinetica.spark.util;

import com.gpudb.Type;
import com.kinetica.exception.KineticaException;

import java.io.Serializable;

/**
 * Class/Object used during SparkLoader.KineticaWriter to load parameters from
 * Created by sunilemanjee on 9/10/17.
 */
public class LoaderParams implements Serializable{

    private String GPUdbURL;
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
    private String jdbcURL;

    public LoaderParams() {
    }

    /**
     * Constructor
     * @param KineticaURL - Kinetica URL including http/https and port
     * @param tablename - table name
     * @param isTableReplicated - true/false
     * @param gpudbIpRegex ip regex
     * @param insertSize bulk inserter insert size
     * @param updateOnExistingPk - true/false Update if primary key found
     * @param kauth - true/false authentication enabled
     * @param kusername - username
     * @param kpassword - password
     * @param threads - Number of bulker insert threads
     */
    public LoaderParams(String KineticaURL, String tablename, boolean isTableReplicated, String gpudbIpRegex, int insertSize, boolean updateOnExistingPk, boolean kauth, String kusername, String kpassword, int threads) {
        this.GPUdbURL = KineticaURL;
        this.tablename = tablename;
        this.isTableReplicated = isTableReplicated;
        this.gpudbIpRegex = gpudbIpRegex;
        this.insertSize = insertSize;
        this.updateOnExistingPk = updateOnExistingPk;
        this.kauth = kauth;
        this.kusername = kusername;
        this.kpassword = kpassword;
        this.threads = threads;

        valdiateUserNamePassword();
    }


    /**
     * Constructor - Full set to parameters
     * @param KineticaURL - Kinetica URL including http/https and port
     * @param jdbcURL - Kinetica JDBC URL including parentSet
     * @param tablename - table name
     * @param isTableReplicated - true/false
     * @param gpudbIpRegex ip regex
     * @param insertSize bulk inserter insert size
     * @param updateOnExistingPk - true/false Update if primary key found
     * @param kauth - true/false authentication enabled
     * @param kusername - username
     * @param kpassword - password
     * @param threads - Number of bulker insert threads
     *
     */
    public LoaderParams(String KineticaURL, String jdbcURL, String tablename, boolean isTableReplicated, String gpudbIpRegex, int insertSize, boolean updateOnExistingPk, boolean kauth, String kusername, String kpassword, int threads) {
        this.GPUdbURL = KineticaURL;
        this.tablename = tablename;
        this.isTableReplicated = isTableReplicated;
        this.gpudbIpRegex = gpudbIpRegex;
        this.insertSize = insertSize;
        this.updateOnExistingPk = updateOnExistingPk;
        this.kauth = kauth;
        this.kusername = kusername;
        this.kpassword = kpassword;
        this.threads = threads;
        this.jdbcURL = jdbcURL;

        valdiateUserNamePassword();
    }



    /**
     * Constructor - used with default params for insertSize, threads, and null ipregex
     * @param KineticaURL - Kinetica URL including http/https and port
     * @param tablename - table name
     * @param isTableReplicated - true/false
     * @param updateOnExistingPk - true/false Update if primary key found
     * @param kauth - true/false authentication enabled
     * @param kusername - username
     * @param kpassword - password
     */
    public LoaderParams(String KineticaURL, String tablename, boolean isTableReplicated, boolean updateOnExistingPk, boolean kauth, String kusername, String kpassword) {
        this.GPUdbURL = KineticaURL;
        this.tablename = tablename;
        this.isTableReplicated = isTableReplicated;
        this.gpudbIpRegex = null;
        this.insertSize = 10000;
        this.updateOnExistingPk = updateOnExistingPk;
        this.kauth = kauth;
        this.kusername = kusername;
        this.kpassword = kpassword;
        this.threads = 4;
        valdiateUserNamePassword();
    }



    /**
     * Constructor - No authentication, with default params for insertSize, threads, and null ipregex
     * @param KineticaURL Kinetica URL including http/https and port
     * @param tablename Kinetica table name
     * @param isTableReplicated true or false is table replicated
     * @param updateOnExistingPk true or false to update if Primary key found
     */
    public LoaderParams(String KineticaURL, String tablename, boolean isTableReplicated, boolean updateOnExistingPk) {
        this.GPUdbURL = KineticaURL;
        this.tablename = tablename;
        this.isTableReplicated = isTableReplicated;
        this.gpudbIpRegex = null;
        this.insertSize = 10000;
        this.updateOnExistingPk = updateOnExistingPk;
        this.kauth = false;
        this.kusername = "admin";
        this.kpassword = "admin";
        this.threads = 4;

        valdiateUserNamePassword();
    }

    private void valdiateUserNamePassword() throws KineticaException{
        if(kauth)
        {
            if(kusername.trim().length()<1)
                throw new KineticaException("Invalid Username");
            if(kpassword.trim().length()<1)
                throw new KineticaException("Invalid password");
        }
    }

    /**
     * Return Kinetica table type
     * @return Type
     */
    public Type getTableType() {
        return tableType;
    }


    /**
     * Set Kinetica Table Type
     * @param tableType Kinetica Table Type
     */
    public void setTableType(Type tableType) {
        this.tableType = tableType;
    }

    /**
     * Get table able
     * @return table name
     */
    public String getTablename() {
        return tablename;
    }

    /**
     * Set table name
     * @param tablename table name
     */
    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    /**
     * Return boolean table replicated, or using single head ingest
     * @return is table replicated true/false boolean
     */
    public boolean getTableReplicated() {
        return isTableReplicated;
    }

    /**
     * Set if table is replication or single head ingest
     * @param tableReplicated boolean true/false
     */
    public void setTableReplicated(boolean tableReplicated) {
        isTableReplicated = tableReplicated;
    }

    /**
     * get regex for worker ip
     * @return regex
     */
    public String getGpudbIpRegex() {
        return gpudbIpRegex;
    }

    /**
     * set Regex for worker ip
     * @param gpudbIpRegex regex
     */
    public void setGpudbIpRegex(String gpudbIpRegex) {
        this.gpudbIpRegex = gpudbIpRegex;
    }

    /**
     * Get Bulkinserter flush size
     * @return flush size
     */
    public int getInsertSize() {
        return insertSize;
    }

    /**
     * Set Bulkinserter flush size
     * @param insertSize flush size
     */
    public void setInsertSize(int insertSize) {
        this.insertSize = insertSize;
    }

    /**
     * Get if update on PK
     * @return true/false boolean
     */
    public boolean getUpdateOnExistingPk() {
        return updateOnExistingPk;
    }

    /**
     * Set if update on PK durng ingestion
     * @param updateOnExistingPk true/false boolean
     */
    public void setUpdateOnExistingPk(boolean updateOnExistingPk) {
        this.updateOnExistingPk = updateOnExistingPk;
    }

    /**
     * Get if auth enabled
     * @return true/false boolean
     */
    public boolean getKauth() {
        return kauth;
    }

    /**
     * Set is auth enabled
     * @param kauth true/false boolean
     */
    public void setKauth(boolean kauth) {
        this.kauth = kauth;
    }

    /**
     * Get username
     * @return username
     */
    public String getKusername() {
        return kusername;
    }

    /**
     * Set username
     * @param kusername username
     */
    public void setKusername(String kusername) {
        this.kusername = kusername;
    }

    /**
     * Get password
     * @return password
     */
    public String getKpassword() {
        return kpassword;
    }

    /**
     * Set password
     * @param kpassword password
     */
    public void setKpassword(String kpassword) {
        this.kpassword = kpassword;
    }

    /**
     * Get number of Kinetica threads during ingestion
     * @return threads
     */
    public int getThreads() {
        return threads;
    }

    /**
     * Set number of threads used during ingestino
     * @param threads number of threads
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Get Kinetica url including port
     * @return Kinetica URL
     */
    public String getGPUdbURL() {
        return GPUdbURL;
    }

    /**
     * Set Ingestion Kinetica URL
     * Including http https and port
     * @param GPUdbURL Kinetica URL including http/https and port
     */
    public void setGPUdbURL(String GPUdbURL) {
        this.GPUdbURL = GPUdbURL;
    }

    public boolean isTableReplicated() {
        return isTableReplicated;
    }

    public boolean isUpdateOnExistingPk() {
        return updateOnExistingPk;
    }

    public boolean isKauth() {
        return kauth;
    }

    public String getJdbcURL() {
        return jdbcURL;
    }

    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }
}
