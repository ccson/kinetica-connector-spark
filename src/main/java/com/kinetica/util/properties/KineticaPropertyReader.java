package com.kinetica.util.properties;

import com.kinetica.exception.KineticaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class KineticaPropertyReader {

	private final static Logger logger =
		LoggerFactory.getLogger(KineticaPropertyReader.class);

	private static Properties properties = null;

	//Private Constructor to avoid initiation
	private KineticaPropertyReader() {
    }

	public static Properties getInstance() {
		if (properties == null) {
			throw new KineticaException("Please initialize the property file.");
		}
		return properties;
	}

	public static void init(InputStream propStream) {

		try {
			properties = new Properties();
			properties.load(propStream);
		} catch (Exception e) {
			e.printStackTrace();
			throw new KineticaException(e);
		}
	}

	public static void init(String propertiesFile) {
		FileInputStream inputStream = null;
		try {
			logger.info("Loading properties from file: " + propertiesFile);
			inputStream = new FileInputStream(propertiesFile);
			properties = new Properties();
			properties.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
			throw new KineticaException(e);
		}
		finally {
			try {
				inputStream.close();
			} catch (Exception e) {
				throw new KineticaException(e);
			}
		}
	}
}