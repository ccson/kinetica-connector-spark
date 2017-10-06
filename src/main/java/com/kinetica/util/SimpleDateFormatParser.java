package com.kinetica.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

/* We need to create SimpleDateFormats for each thread because they are not thread safe.
 * In addition, creating an instance per thread will remove the overhead of "synchronization"
 * This method will check to see if we already have created the SimpleDateFormat
 that matches the dateFormatString for that thread and if so will retrieve it.  
 If not, we'll create it and store it in our dateFormats collection. One for every threadId
*/

public class SimpleDateFormatParser {

	private static HashMap<String, SimpleDateFormat> 
		dateFormats = new HashMap<String, SimpleDateFormat>();

	//Private Constructor to avoid initiation
	private SimpleDateFormatParser() {
    }

	private synchronized static SimpleDateFormat getSimpleDateFormat(
			String threadId, String dateFormat) {
		String key = threadId + "-" + dateFormat;
		if (!dateFormats.containsKey(key)) {
			dateFormats.put(key, new SimpleDateFormat(dateFormat));
		}
		return dateFormats.get(key);
	}

	public static Long getTimestamp(
			String threadId, String dateString,
				String dateFormat) throws ParseException {
		SimpleDateFormat format = getSimpleDateFormat(threadId, dateFormat);
		return format.parse(dateString).getTime();
	}
}