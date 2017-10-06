package com.kinetica.util;


import com.kinetica.exception.KineticaException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCConnectionUtils {

	private static Connection connection = null;
	private static Statement stmt = null;

	/**
	 * Initialize JDBCConnectionUtils
	 * @param url full jdbc url with parentset
	 */
	public static void Init(String url) {
		try {
			connection = createKineticaConnection(url);
			//set this connection to the local variable, so that we can close it later.
			stmt = connection.createStatement();
		} catch (SQLException e) {
			throw new KineticaException(e);
		}
	}

	private static Connection createKineticaConnection(String url) {
		try {
			return getConnection("com.simba.client.core.jdbc4.SCJDBC4Driver", url);
		} catch (ClassNotFoundException e) {
			throw new KineticaException("Invalid driver class name: " + "com.simba.client.core.jdbc4.SCJDBC4Driver", e);
		} catch (SQLException e) {
			throw new KineticaException("Cannot connect: ", e);
		}
	}

	private static Connection getConnection(String driverClass, String url) throws ClassNotFoundException, SQLException {
		Class.forName(driverClass);
		Properties p = new Properties();
		p.setProperty("UID", "admin");
		p.setProperty("PWD", "admin");

		return DriverManager.getConnection(url, p);
	}

	/**
	 * Close connection and statement
	 */
	public static void close() {
		try {
			if (stmt != null && !stmt.isClosed()) {
				stmt.close();
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (Exception e) {
			throw new KineticaException("Unable to close statement or connection ", e);
		}
	}

	/**
	 * Call init prior to call this method
	 * Method will execute sql
	 * @param sqlStatement
	 * @throws KineticaException
	 */

	public static void executeSQL(String sqlStatement) throws KineticaException{
		try {
			stmt.execute(sqlStatement);
		} catch (Exception e)
		{
			e.printStackTrace();
			throw new KineticaException("SQL failed: "+sqlStatement, e);

		}
	}

}
