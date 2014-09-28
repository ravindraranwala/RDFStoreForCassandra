package com.msc.research.cassandra.util;

public class NumberUtil {

	public static boolean isLong(String s) {
		try {
			if (s.contains(",")) {
				s = new StringBuffer(s).deleteCharAt(s.indexOf(",")).toString();
			}
			Long.parseLong(s);
		} catch (NumberFormatException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
	}

	public static boolean isDouble(String s) {
		try {
			Double.parseDouble(s);
		} catch (NumberFormatException e) {
			return false;
		}

		return true;
	}

}
