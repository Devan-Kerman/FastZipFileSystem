package net.devtech.fastzipfilesystem;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.PatternSyntaxException;

class FastZipUtil {
	static int ceilDiv(long numerator, long denominator) {
		return Math.toIntExact((numerator + denominator - 1) / denominator);
	}
	
	static final String regexMetaChars = ".^$+{[]|()";
	static final String globMetaChars = "\\*?[{";
	
	static boolean isRegexMeta(char c) {
		return regexMetaChars.indexOf(c) != -1;
	}
	
	static boolean isGlobMeta(char c) {
		return globMetaChars.indexOf(c) != -1;
	}
	
	static char EOL = 0;  //TBD
	
	static char next(String glob, int i) {
		if(i < glob.length()) {
			return glob.charAt(i);
		}
		return EOL;
	}
	
	/*
	 * Creates a regex pattern from the given glob expression.
	 *
	 * @throws  PatternSyntaxException
	 */
	static String toRegexPattern(String globPattern) {
		boolean inGroup = false;
		StringBuilder regex = new StringBuilder("^");
		
		int i = 0;
		while(i < globPattern.length()) {
			char c = globPattern.charAt(i++);
			switch(c) {
				case '\\':
					// escape special characters
					if(i == globPattern.length()) {
						throw new PatternSyntaxException("No character to escape", globPattern, i - 1);
					}
					char next = globPattern.charAt(i++);
					if(isGlobMeta(next) || isRegexMeta(next)) {
						regex.append('\\');
					}
					regex.append(next);
					break;
				case '/':
					regex.append(c);
					break;
				case '[':
					// don't match name separator in class
					regex.append("[[^/]&&[");
					if(next(globPattern, i) == '^') {
						// escape the regex negation char if it appears
						regex.append("\\^");
						i++;
					} else {
						// negation
						if(next(globPattern, i) == '!') {
							regex.append('^');
							i++;
						}
						// hyphen allowed at headerStart
						if(next(globPattern, i) == '-') {
							regex.append('-');
							i++;
						}
					}
					boolean hasRangeStart = false;
					char last = 0;
					while(i < globPattern.length()) {
						c = globPattern.charAt(i++);
						if(c == ']') {
							break;
						}
						if(c == '/') {
							throw new PatternSyntaxException("Explicit 'name separator' in class", globPattern, i - 1);
						}
						// TBD: how to specify ']' in a class?
						if(c == '\\' || c == '[' || c == '&' && next(globPattern, i) == '&') {
							// escape '\', '[' or "&&" for regex class
							regex.append('\\');
						}
						regex.append(c);
						
						if(c == '-') {
							if(!hasRangeStart) {
								throw new PatternSyntaxException("Invalid range", globPattern, i - 1);
							}
							if((c = next(globPattern, i++)) == EOL || c == ']') {
								break;
							}
							if(c < last) {
								throw new PatternSyntaxException("Invalid range", globPattern, i - 3);
							}
							regex.append(c);
							hasRangeStart = false;
						} else {
							hasRangeStart = true;
							last = c;
						}
					}
					if(c != ']') {
						throw new PatternSyntaxException("Missing ']", globPattern, i - 1);
					}
					regex.append("]]");
					break;
				case '{':
					if(inGroup) {
						throw new PatternSyntaxException("Cannot nest groups", globPattern, i - 1);
					}
					regex.append("(?:(?:");
					inGroup = true;
					break;
				case '}':
					if(inGroup) {
						regex.append("))");
						inGroup = false;
					} else {
						regex.append('}');
					}
					break;
				case ',':
					if(inGroup) {
						regex.append(")|(?:");
					} else {
						regex.append(',');
					}
					break;
				case '*':
					if(next(globPattern, i) == '*') {
						// crosses directory boundaries
						regex.append(".*");
						i++;
					} else {
						// within directory boundary
						regex.append("[^/]*");
					}
					break;
				case '?':
					regex.append("[^/]");
					break;
				default:
					if(isRegexMeta(c)) {
						regex.append('\\');
					}
					regex.append(c);
			}
		}
		if(inGroup) {
			throw new PatternSyntaxException("Missing '}", globPattern, i - 1);
		}
		return regex.append('$').toString();
	}
	
	static int decode(char c) {
		if((c >= '0') && (c <= '9')) {
			return c - '0';
		}
		if((c >= 'a') && (c <= 'f')) {
			return c - 'a' + 10;
		}
		if((c >= 'A') && (c <= 'F')) {
			return c - 'A' + 10;
		}
		assert false;
		return -1;
	}
	
	public static long javaToDosTime(long time) {
		Instant instant = Instant.ofEpochMilli(time);
		LocalDateTime ldt = LocalDateTime.ofInstant(
				instant, ZoneId.systemDefault());
		int year = ldt.getYear() - 1980;
		if (year < 0) {
			return (1 << 21) | (1 << 16);
		}
		return (year << 25 |
		        ldt.getMonthValue() << 21 |
		        ldt.getDayOfMonth() << 16 |
		        ldt.getHour() << 11 |
		        ldt.getMinute() << 5 |
		        ldt.getSecond() >> 1) & 0xffffffffL;
	}
	
	public static long dosToJavaTime(short a, short b) {
		long dtime = (a | (b << 16)) & 0xFFFFFFFFL;
		return dosToJavaTime(dtime);
	}
	
	public static long dosToJavaTime(long dtime) {
		int year = (int) (((dtime >> 25) & 0x7f) + 1980);
		int month = (int) ((dtime >> 21) & 0x0f);
		int day = (int) ((dtime >> 16) & 0x1f);
		int hour = (int) ((dtime >> 11) & 0x1f);
		int minute = (int) ((dtime >> 5) & 0x3f);
		int second = (int) ((dtime << 1) & 0x3e);
		
		if (month > 0 && month < 13 && day > 0 && hour < 24 && minute < 60 && second < 60) {
			try {
				LocalDateTime ldt = LocalDateTime.of(year, month, day, hour, minute, second);
				return TimeUnit.MILLISECONDS.convert(ldt.toEpochSecond(
						ZoneId.systemDefault().getRules().getOffset(ldt)), TimeUnit.SECONDS);
			} catch (DateTimeException dte) {
				// ignore
			}
		}
		return overflowDosToJavaTime(year, month, day, hour, minute, second);
	}
	
	private static long overflowDosToJavaTime(int year, int month, int day,
	                                          int hour, int minute, int second) {
		return new Date(year - 1900, month - 1, day, hour, minute, second).getTime();
	}
	
	static String toStr(ByteBuffer buffer) {
		int start = buffer.position();
		CharBuffer decode = UTF_8.decode(buffer);
		buffer.position(start);
		return decode + "";
	}
	
	// to avoid double escape
	static String decodeUri(String s) {
		if(s == null) {
			return null;
		}
		int n = s.length();
		if(n == 0) {
			return s;
		}
		if(s.indexOf('%') < 0) {
			return s;
		}
		
		StringBuilder sb = new StringBuilder(n);
		byte[] bb = new byte[n];
		boolean betweenBrackets = false;
		
		for(int i = 0; i < n; ) {
			char c = s.charAt(i);
			if(c == '[') {
				betweenBrackets = true;
			} else if(betweenBrackets && c == ']') {
				betweenBrackets = false;
			}
			if(c != '%' || betweenBrackets) {
				sb.append(c);
				i++;
				continue;
			}
			int nb = 0;
			while(c == '%') {
				assert (n - i >= 2);
				bb[nb++] = (byte) (((decode(s.charAt(++i)) & 0xf) << 4) | (decode(s.charAt(++i)) & 0xf));
				if(++i >= n) {
					break;
				}
				c = s.charAt(i);
			}
			sb.append(new String(bb, 0, nb, UTF_8));
		}
		return sb.toString();
	}
}
