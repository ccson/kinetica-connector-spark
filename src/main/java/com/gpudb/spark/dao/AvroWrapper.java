package com.gpudb.spark.dao;

import com.gpudb.Avro;
import com.gpudb.GPUdbException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Wrapper for result data returned by a GPUdb table monitor and the
 * corresponding data schema
 * 
 * @author dkatz
 */
public class AvroWrapper implements Serializable
{
	private static final long serialVersionUID = 5845927120287011691L;

	private byte[] bytes;
	private String schemaStr;

	/**
	 * Creates a wrapper of the given payload with the given schema
	 * 
	 * @param schemaStr schema associated with data
	 * @param bytes data
	 */
	public AvroWrapper(String schemaStr, byte[] bytes)
	{
		this.bytes = bytes;
		this.schemaStr = schemaStr;
	}

	/**
	 * Decodes the data with the schema, returning an Avro record
	 * 
	 * @return Avro record decoded from the source data with the source schema
	 * @throws GPUdbException if an error occurs decoding source data
	 */
	public GenericRecord getGenericRecord() throws GPUdbException
	{
		return Avro.decode(new Schema.Parser().setValidate(true).parse(schemaStr), ByteBuffer.wrap(bytes));
	}
	
	@Override
	public String toString()
	{
		String retVal = null;

		try
		{
			retVal = getGenericRecord().toString();
		}
		catch (GPUdbException e)
		{
			throw new RuntimeException("Problem decoding record", e);
		}

		return retVal;
	}
}
