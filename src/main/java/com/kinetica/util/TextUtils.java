package com.kinetica.util;

import java.io.Serializable;
import java.text.ParseException;

public class TextUtils implements Serializable {

	public static Integer getInt(String value) {
		if (value == null || value.equalsIgnoreCase("null") || value.trim().equals("")) {
			return null;
		} else {
			return Integer.parseInt(value.trim());
		}
	}

	public static Long getLong(String value) {
		if (value == null || value.equalsIgnoreCase("null") || value.trim().equals("")) {
			return null;
		} else {
			return Long.parseLong(value.trim());
		}
	}

	public static Float getFloat(String value) {
		if (value == null || value.equalsIgnoreCase("null") || value.trim().equals("")) {
			return null;
		} else {
			return Float.parseFloat(value.trim());
		}
	}

	public static String getString(String value) {
		if (value == null || value.trim().equalsIgnoreCase("null") || value.trim().equalsIgnoreCase("")) {
			return null;
		// special case check for tab character because trim() will delete "\t"
		} else if (value.equalsIgnoreCase("\t")) {
			return value;
		} else if (value.trim().equals("")) {
			return null;
		} else {
			return value.trim();
		}
	}

	public static Boolean getBoolean(String value) throws ParseException {
		if (value == null || value.trim().length() == 0 || value.trim().equalsIgnoreCase("null")) {
			return null;
		} else if (value.trim().equalsIgnoreCase("true")) {
			return true;
		} else if (value.trim().equalsIgnoreCase("false")) {
			return false;
		} else {
			throw new ParseException("Error parsing value  '" + value + "' as boolean", 0);
		}
	}
}
