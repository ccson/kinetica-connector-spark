package com.kinetica.util;

public class Statistics {

	private static int totalRecordsInserted = 0;
	private static int totalRecordsUpdated = 0;
	private static int totalRecordsRejected = 0;

	public static void setTotalRecordsInserted(int _totalRecordsInserted) {
		totalRecordsInserted = _totalRecordsInserted;
	}

	public static void setTotalRecordsRejected(int _totalRecordsRejected) {
		totalRecordsRejected = _totalRecordsRejected;
	}

	public static void addToTotalRecordsInserted(long numRecords) {
		totalRecordsInserted += numRecords;
	}

	public static void setTotalRecordsUpdated(int _totalRecordsUpdated) {
		totalRecordsUpdated = _totalRecordsUpdated;
	}

	public static void addToTotalRecordsUpdated(long numRecords) {
		totalRecordsUpdated += numRecords;
	}

	public static long getTotalRecordsInserted() {
		return totalRecordsInserted;
	}

	public static long getTotalRecordsUpdated() {
		return totalRecordsUpdated;
	}
	
	public static void incrementTotalRecordsRejected() {
		totalRecordsRejected += 1;
	}

	public static long getTotalRecordsRejected() {
		return totalRecordsRejected;
	}

}
