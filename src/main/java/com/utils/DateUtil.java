package com.utils;

import java.text.ParseException;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang3.time.FastDateFormat;

public class DateUtil {
	private static FastDateFormat fdin = null;
	private static FastDateFormat fdout = null;
	static {
		fdin = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
		fdout = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
	}

	public static String parse(String time) {

		return fdout.format(new Date(getTime(time)));
	}

	private static long getTime(String time) {
		try {
			long date = fdin.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime();
			return date;
		} catch (ParseException e) {
			return 0l;
		}
	}
	
	public static void main(String[] args) {
		System.out.println(parse("[10/Nov/2016:00:01:02 +0800]"));
	}
}
