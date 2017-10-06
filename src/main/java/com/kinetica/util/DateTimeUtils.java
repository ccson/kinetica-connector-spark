package com.kinetica.util;

import com.kinetica.exception.KineticaException;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class DateTimeUtils {

	public final static Logger log = LoggerFactory.getLogger(DateTimeUtils.class);

	public static long getLongMillis(String dateString, String[] parsePatterns) {
		try {
			return DateUtils.parseDate(dateString, parsePatterns).getTime();
		} catch (ParseException e) {
			throw new KineticaException("Could not parse date. DateString is :" + dateString + ":", e);
		}
	}
}