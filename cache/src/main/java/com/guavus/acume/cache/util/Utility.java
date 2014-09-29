package com.guavus.acume.cache.util;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.shiro.SecurityUtils;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rubix.exception.RubixException;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;
import util.io.IoUtil;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.guavus.bloomfilter.BloomFilterOperatorStaticFactory;
import com.guavus.bloomfilter.IBloomFilterOperator;
import com.guavus.rubix.cache.Interval;
import com.guavus.rubix.cache.Intervals;
import com.guavus.rubix.cache.TimeGranFinder;
import com.guavus.rubix.cache.TimeGranularity;
import com.guavus.rubix.cds.ByteBuffer;
import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.AggregationContext;
import com.guavus.rubix.core.DataMergeType;
import com.guavus.rubix.core.ICube;
import com.guavus.rubix.core.IDimension;
import com.guavus.rubix.core.IMeasure;
import com.guavus.rubix.core.INumericFunction;
import com.guavus.rubix.core.PrefetchConfiguration;
import com.guavus.rubix.core.distribution.TopologyMismatchException;
import com.guavus.rubix.filter.FilterOperation;
import com.guavus.rubix.parameters.FilterObject.SingleFilter;
import com.guavus.rubix.query.IGenericDimension;
import com.guavus.rubix.query.remote.flex.TimeZoneInfo;
import com.guavus.rubix.rules.IRule;
import com.guavus.rubix.rules.TimeBasedRuleServiceUtility;
import com.guavus.rubix.workflow.IDependentMeasuresContainer;
import com.guavus.rubix.workflow.IMeasureProcessor;
import com.guavus.rubix.workflow.IMeasureProcessorMap;
import com.guavus.rubix.workflow.IRequest;
import com.guavus.rubix.workflow.Request;

public class Utility {

public static Logger logger = LoggerFactory.getLogger(Utility.class);
	
	public static String[] validThreadPatternNames = 
			RubixProperties.ThreadPatternsForMemoryReading.getStringArray(",");
	public static TimeZone timeZone = Utility.getTimeZone(RubixProperties.TimeZone.getValue());
	public static TimeZone utcTimeZone = Utility.getTimeZone("UTC");
	private static Calendar calendar = Calendar.getInstance(timeZone);
	public static Calendar utcCalendar = Calendar.getInstance(utcTimeZone);


	private static final int MILLIS_IN_SECOND = 1000;
	private static final int SECS_IN_HOUR = 3600;
	private static final int HOUR_MAX = 24;
	private static Timer timerService = new Timer();
	
	
	public static void refreshTimeZoneInfo(){
		timeZone = Utility.getTimeZone(RubixProperties.TimeZone.getValue());
		utcTimeZone = Utility.getTimeZone("UTC");
		calendar = Calendar.getInstance(timeZone);
		utcCalendar = Calendar.getInstance(utcTimeZone);
	}

	public static final java.nio.ByteBuffer EMPTY_NIO_BUFFER;
	public static final ByteBuffer EMPTY_BUFFER;
    public static IBloomFilterOperator BLOOM_FILTER_OP = BloomFilterOperatorStaticFactory.create();
	
	static {
		EMPTY_NIO_BUFFER = java.nio.ByteBuffer.allocate(8);
		EMPTY_NIO_BUFFER.put(0, (byte)-2);
		EMPTY_BUFFER = new ByteBuffer(EMPTY_NIO_BUFFER);
		RubixProperties.TimeZone.addObserver(new Observer() {
			
			@Override
			public void update(Observable o, Object arg) {
				refreshTimeZoneInfo();
				
			}
		});
	}
	
	public static Timer getTimerService() {
		return Utility.timerService;
	}
	
    /**
     * Method to differentiate between query from scheduler or UI.
     *
     * @return false if query is from scheduler true if query from UI.
     */
    public static boolean isSchedulerSession() {
        try{
        	if(SecurityUtils.getSubject().getSession(false) == null)
        		return true;
        	} catch(Exception e) {
        		return true;
        	}
        return false;
    }
	@Deprecated
	public static long getFloorToLevel(long time, long level) {
		long parentInterval = (time/level) * level;
		return parentInterval;
	}

	public static long floor(long time, TimeGranularity timeGranularity) {
		return floor(time, timeGranularity.getGranularity());
	}

	public static long ceiling(long time, TimeGranularity timeGranularity) {
		return ceiling(time, timeGranularity.getGranularity());
	}

	public static int ceiling(int time, TimeGranularity timeGranularity) {
		long result = ceiling((long)time, timeGranularity.getGranularity());
		if(result > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;

		return (int)result;
	}

	public static long floor(long time, long round) {
		return (time / round) * round;
	}

	public static long ceiling(long time, long round) {
		if (time % round != 0) {
			time = ((time / round) + 1) * round;
		}
		return time;
	}

	public static boolean validString(String str)
	{
		if(str == null || str.isEmpty()){
			return false;
		}else{
			return true;
		}
	}

	public static boolean equalString(String str1, String str2)
	{
		return ((str1 == null  && str2 == null) || (str1 != null && str2!= null &&
				str1.equals(str2)));
	}
	public static <S extends Collection<Integer>, T extends Map<Long, double[]>> Map<S, double[]> convertToAggregate(
			Map<S, T> measures) {
		Map<S, double[]> retVal = new HashMap<S, double[]>();
		for (Entry<S, T> e : measures.entrySet()) {
			retVal.put(e.getKey(), e.getValue().values().iterator().next());
		}
		return retVal;
	}
	
	public static String humanReadableTimeInterval(long startTime, long endTime) {
		return humanReadableTime(endTime - startTime);
	}

	public static String humanReadableTime(long time) {
		StringBuilder sb = new StringBuilder();
		long minutes = time / (1000 * 60);
		if (minutes != 0)
			sb.append(minutes).append(" minutes ");
		long seconds = (time / 1000) % 60;
		if (seconds != 0)
			sb.append(seconds).append(" seconds ");
		long millis = time % 1000;
		sb.append(millis).append(" milliseconds");
		return sb.toString();
	}

	public static String humanReadableTimeStamp(long timestampInSeconds) {
		DateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss z");
		dateFormat.setTimeZone(utcTimeZone);
		return dateFormat.format(new Date(timestampInSeconds * 1000));
	}

	private static int[] getTimestampCount(Calendar start, Calendar end,
			TimeGranularity dataTimeGranularity) {
		Calendar startCal = (Calendar) start.clone();
		Calendar endCal = (Calendar) end.clone();

		int[] hourlyIntervals = new int[24];

		while (startCal.before(endCal)) {
			int hour = startCal.get(Calendar.HOUR_OF_DAY);

			hourlyIntervals[hour]++;

			// increase by dataTimeGranularity
			startCal.setTimeInMillis(startCal.getTimeInMillis()
					+ dataTimeGranularity.getGranularity() * MILLIS_IN_SECOND);
		}

		return hourlyIntervals;
	}

	private static int getWeekdayCount(Calendar start, Calendar end) {
		Calendar startCal = (Calendar) start.clone();
		Calendar endCal = (Calendar) end.clone();

		int numberOfWeekdays = 0;

		while (startCal.before(endCal)) {
			int startDay = startCal.get(Calendar.DAY_OF_WEEK);
			if (startDay != Calendar.SATURDAY && startDay != Calendar.SUNDAY) {
				numberOfWeekdays++;
			}

			// increment date by 1
			startCal.add(Calendar.DATE, 1);
		}

		return numberOfWeekdays;
	}

	public static Calendar getDayAfterGivenTime(Calendar startDate, int incr) {
		Calendar cal = (Calendar) startDate.clone();
		cal.add(Calendar.DATE, incr);
		cal.set(Calendar.HOUR_OF_DAY,
				cal.getActualMinimum(Calendar.HOUR_OF_DAY));
		cal.set(Calendar.MINUTE, cal.getActualMinimum(Calendar.MINUTE));
		cal.set(Calendar.SECOND, cal.getActualMinimum(Calendar.SECOND));
		return cal;
	}

	public static int getFutureDayShift(Calendar cal, int destDay) {
		int currDay = cal.get(Calendar.DAY_OF_WEEK);
		int maxDays = cal.getActualMaximum(Calendar.DAY_OF_WEEK);
		return (destDay == currDay) ? maxDays : (destDay - currDay + maxDays)
				% maxDays;
	}
	
	public static void printMemoryStats() {
//		logInfo("FreeMemory before calling GC: {}", Runtime.getRuntime()
//				.freeMemory());
		System.gc();
		System.gc();
		logger.info("FreeMemory after calling GC:  {}", Runtime.getRuntime()
				.freeMemory());
	}

	public static double computePercentage(double numerator, double denominator) {
		return (numerator / denominator) * 100;
	}
	
	public static double computeGrowth(double numerator, double denominator) {
		if (denominator == 0) {
			return -200d;
		} else {
			double growth = ((numerator - denominator) / denominator) * 100;
			return growth;
		}
	}

	public static long[] getPreviousMonthStartEndTime(long startTime) {
		Calendar calendar = newCalendar();
		// convert to milliseconds..input coming as seconds
		calendar.setTimeInMillis(startTime * 1000);
		// roll one month back.
		calendar.add(Calendar.MONTH, -1);
		long[] timeStamps = new long[] { getStartTimeOfMonth(calendar),
				getEndTimeOfMonth(calendar) };
		return timeStamps;
	}

	public static long[] getMonthStartEndTime(long startTime) {
		Calendar calendar = Utility.newCalendar();
		// convert to milliseconds..input coming as seconds
		calendar.setTimeInMillis(startTime * 1000);

		long[] timeStamps = new long[] { getStartTimeOfMonth(calendar),
				getEndTimeOfMonth(calendar) };
		return timeStamps;
	}

	public static long[] getNextMonthStartEndTime(long startTime) {
		Calendar calendar = Utility.newCalendar();
		// convert to milliseconds..input coming as seconds
		calendar.setTimeInMillis(startTime * 1000);
		// add one month
		calendar.add(Calendar.MONTH, 1);
		long[] timeStamps = new long[] { getStartTimeOfMonth(calendar),
				getEndTimeOfMonth(calendar) };
		return timeStamps;
	}

	private static long getEndTimeOfMonth(Calendar cal) {
		int maximumDateMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		cal.set(Calendar.DAY_OF_MONTH, maximumDateMonth);
		cal.set(Calendar.HOUR_OF_DAY, 24);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTimeInMillis() / 1000;

	}
	
	public static long getDayFromOffset(long time, TimeZone timeZone, int dayOff) {
		Calendar instance = Calendar.getInstance(timeZone);
		instance.setTimeInMillis(time * 1000);
		instance.add(Calendar.DATE, dayOff);
		return instance.getTimeInMillis() / 1000;
	}

	private static long getStartTimeOfMonth(Calendar cal) {
		int minimumDatePrevMonth = cal.getActualMinimum(Calendar.DAY_OF_MONTH);
		cal.set(Calendar.DAY_OF_MONTH, minimumDatePrevMonth);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTimeInMillis() / 1000;

	}

	public static long floorToTimeZone(long time, TimeZone timezone,
			TimeGranularity timeGrnaularity) {
		Calendar instance = Calendar.getInstance(timezone);
		return floorToTimezone(time, timeGrnaularity, instance);
	}
	
	public static long floorToTimeZone(long time, TimeGranularity timeGrnaularity) {
		Calendar instance = newCalendar();
		return floorToTimezone(time, timeGrnaularity, instance);
	}

	public static long floorToTimezone(long time,
			TimeGranularity timeGrnaularity, Calendar instance) {
		instance.setTimeInMillis(time * 1000);
		switch (timeGrnaularity) {
		case MONTH:
			int minimumDatePrevMonth = instance
			.getActualMinimum(Calendar.DAY_OF_MONTH);
			instance.set(Calendar.DAY_OF_MONTH, minimumDatePrevMonth);
			instance.set(Calendar.HOUR_OF_DAY, 0);
	    	instance.set(Calendar.MINUTE, 0);
			break;
		case DAY:
			instance.set(Calendar.HOUR_OF_DAY, 0);
			instance.set(Calendar.MINUTE,0);
			break;
		case HALF_DAY:
			int hour = (int) ((instance.get(Calendar.HOUR_OF_DAY) / TimeGranularity.HALF_DAY
					.getDurationInHour()) * TimeGranularity.HALF_DAY
					.getDurationInHour());
			instance.set(Calendar.MINUTE,0);
			instance.set(Calendar.HOUR_OF_DAY,hour);
			break;
		case FOUR_HOUR:
			hour = (int) ((instance.get(Calendar.HOUR_OF_DAY) / TimeGranularity.FOUR_HOUR
					.getDurationInHour()) * TimeGranularity.FOUR_HOUR
					.getDurationInHour());
			instance.set(Calendar.MINUTE,0);
			instance.set(Calendar.HOUR_OF_DAY,hour);
			break;
		case THREE_HOUR:
			hour = (int) ((instance.get(Calendar.HOUR_OF_DAY) / TimeGranularity.THREE_HOUR
					.getDurationInHour()) * TimeGranularity.THREE_HOUR
					.getDurationInHour());
			instance.set(Calendar.MINUTE,0);
			instance.set(Calendar.HOUR_OF_DAY,hour);
			break;
		case HOUR:
			instance.add(Calendar.MINUTE, -1* instance.get(Calendar.MINUTE));
			break;
		case FIVE_MINUTE:
			int minute = (int) ((instance.get(Calendar.MINUTE) / TimeGranularity.FIVE_MINUTE
					.getDurationInMinutes()) * TimeGranularity.FIVE_MINUTE
					.getDurationInMinutes());
			instance.add(Calendar.MINUTE, -1 * (instance.get(Calendar.MINUTE) - minute));
			break;
		case FIFTEEN_MINUTE:
			 minute = (int) ((instance.get(Calendar.MINUTE) / TimeGranularity.FIFTEEN_MINUTE
					.getDurationInMinutes()) * TimeGranularity.FIFTEEN_MINUTE
					.getDurationInMinutes());
			 instance.add(Calendar.MINUTE, -1 * (instance.get(Calendar.MINUTE) - minute));
			break;
		case ONE_MINUTE:
			break;
		case TWO_DAYS:
		case THREE_DAYS:
			int days = daysFromReference(instance, instance.getTimeZone());
			if(days < 0) days*=-1;
			int offset = days % (int)timeGrnaularity.getDurationInDay();
			instance.add(Calendar.DAY_OF_MONTH,-1 * offset);
			instance.set(Calendar.MINUTE,0);
			instance.set(Calendar.HOUR_OF_DAY, 0);
			break;
		case WEEK:
			instance.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			instance.set(Calendar.MINUTE,0);
			instance.set(Calendar.HOUR_OF_DAY, 0);
			break;
		default:
			throw new IllegalArgumentException("This " + timeGrnaularity
					+ " floor is not supported. Make changes to support it");
		}
		instance.add(Calendar.SECOND, -1 * instance.get(Calendar.SECOND));
		return instance.getTimeInMillis() / 1000;
	}

	public static long ceilingToTimeZone(long time, TimeZone timezone,
			TimeGranularity timeGrnaularity) {
		Calendar instance = Calendar.getInstance(timezone);
		return ceilingToTimezone(time, timeGrnaularity, instance);
	}
	
	public static long ceilingToTimeZone(long time, TimeGranularity timeGrnaularity) {
		Calendar instance = newCalendar();
		return ceilingToTimezone(time, timeGrnaularity, instance);
	}

	public static long ceilingToTimezone(long time,
			TimeGranularity timeGrnaularity, Calendar instance) {
		instance.setTimeInMillis(time * 1000);
		switch (timeGrnaularity) {
		case MONTH:
			if (instance.get(Calendar.DATE) > 1 || instance.get(Calendar.HOUR_OF_DAY) > 0
					|| instance.get(Calendar.MINUTE) > 0 || instance.get(Calendar.SECOND)>0) {
				int minimumDatePrevMonth = instance
						.getActualMinimum(Calendar.DAY_OF_MONTH);
				instance.set(Calendar.DAY_OF_MONTH, minimumDatePrevMonth);
				instance.add(Calendar.MONTH, 1);
				instance.set(Calendar.HOUR_OF_DAY, 0);
				instance.set(Calendar.MINUTE, 0);
			}
			break;
		case DAY:
			if (instance.get(Calendar.HOUR_OF_DAY) > getMinimumHour(instance) || instance.get(Calendar.MINUTE) > 0 || instance.get(Calendar.SECOND)>0) {
				instance.add(Calendar.DAY_OF_MONTH, 1);
				instance.set(Calendar.HOUR_OF_DAY, 0);
				instance.set(Calendar.MINUTE, 0);
			}
			break;
		case HALF_DAY:
		case FOUR_HOUR:
		case THREE_HOUR:
			long hour = timeGrnaularity.getDurationInHour();
			long hourOfDay = instance.get(Calendar.HOUR_OF_DAY);
			if(hourOfDay % hour != 0 || instance.get(Calendar.MINUTE)>0 || instance.get(Calendar.SECOND)>0){
				hourOfDay = ((hourOfDay / hour) + 1) * hour;
			}
			instance.set(Calendar.HOUR_OF_DAY,(int)hourOfDay);
			instance.set(Calendar.MINUTE,0);
			break;
		case HOUR:
			if (instance.get(Calendar.MINUTE) > 0 || instance.get(Calendar.SECOND) > 0) {
				instance.add(Calendar.HOUR_OF_DAY, 1);
				instance.add(Calendar.MINUTE, -1 * instance.get(Calendar.MINUTE));
			}
			break;
		case FIVE_MINUTE:
			instance.setTimeInMillis(ceiling(instance.getTimeInMillis() / 1000,
					TimeGranularity.FIVE_MINUTE) * 1000);
			break;
		case FIFTEEN_MINUTE:
			instance.setTimeInMillis(ceiling(instance.getTimeInMillis() / 1000,
					TimeGranularity.FIFTEEN_MINUTE) * 1000);
			break;
		case ONE_MINUTE:
			if(instance.get(Calendar.SECOND) > 0){
				instance.add(Calendar.MINUTE, 1);
			}
			break;
		case TWO_DAYS:
		case THREE_DAYS:
			int days = daysFromReference(instance, instance.getTimeZone());
			if(days < 0 && days%timeGrnaularity.getDurationInDay() != 0){
				days*=-1;
			}
			if(days > 0){
				int offset = (int) timeGrnaularity.getDurationInDay() - days % (int)timeGrnaularity.getDurationInDay();
				instance.add(Calendar.DAY_OF_MONTH,offset);
				instance.set(Calendar.MINUTE,0);
				instance.set(Calendar.HOUR_OF_DAY, 0);
			}
			break;
		case WEEK:
			if (instance.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY
					|| instance.get(Calendar.HOUR_OF_DAY) > 0
					|| instance.get(Calendar.MINUTE) > 0
					|| instance.get(Calendar.SECOND) > 0) {
				instance.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
				instance.add(Calendar.DAY_OF_MONTH, Calendar.SATURDAY);
				instance.set(Calendar.MINUTE, 0);
				instance.set(Calendar.HOUR_OF_DAY, 0);
			}
			break;
		default:
			throw new IllegalArgumentException("This " + timeGrnaularity
					+ "ceiling is not supported. Make changes to support it");
		}
		instance.add(Calendar.SECOND, -1 * instance.get(Calendar.SECOND));
		return instance.getTimeInMillis() / 1000;
	}
	
	public static int getMinimumHour(Calendar instance){
		Calendar cal = (Calendar) instance.clone();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		return cal.get(Calendar.HOUR_OF_DAY);
	}
	
	public static int daysFromReference(Calendar startDate, TimeZone timezone)
	{
		Calendar endDate = Calendar.getInstance(timezone);
		endDate.set(Calendar.YEAR, 2000);
		endDate.set(Calendar.MONTH, 0);
		endDate.set(Calendar.DAY_OF_MONTH, 1);
		endDate.set(Calendar.HOUR_OF_DAY, 0);
		endDate.set(Calendar.MINUTE, 0);
		endDate.set(Calendar.SECOND, 0);
		endDate.set(Calendar.MILLISECOND, 0);
		long endTime = endDate.getTimeInMillis();  
		long startTime = startDate.getTimeInMillis();
		int days = (int) ((startTime - endTime) / (1000 * 60 * 60 * 24));  
		endDate.add(Calendar.DAY_OF_YEAR, days);
		if(endDate.getTimeInMillis() == startDate.getTimeInMillis()){
			return -1 * days;
		}
		if(endDate.getTimeInMillis() < startDate.getTimeInMillis()){
			while(endDate.getTimeInMillis() < startDate.getTimeInMillis()){
				endDate.add(Calendar.DAY_OF_MONTH, 1);
				days++;
			}
			if(endDate.getTimeInMillis() == startDate.getTimeInMillis()){
				return -1 * days;
			}
			else{
				return days - 1;
			}

		}
		else{
			while(endDate.getTimeInMillis() > startDate.getTimeInMillis()){
				endDate.add(Calendar.DAY_OF_MONTH, -1);
				days--;
			}
			if(endDate.getTimeInMillis() == startDate.getTimeInMillis()){
				return -1 * days;
			}
			else{
				return days;
			}

		}

	}

	public static long getStartTimeOfDay(Calendar cal) {
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis() / 1000;
	}

	public static long getStartTimeOfNextDay() {
		Calendar cal = newCalendar();
		cal.add(Calendar.DAY_OF_YEAR, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis() / 1000;
	}

	public static <T extends Collection<?>> boolean isNullOrEmpty(T t) {
		return t == null || t.isEmpty();
	}

	public static <K, V> boolean isNullOrEmpty(Map<K, V> map) {
		return map == null || map.isEmpty();
	}

	public static boolean isNullOrEmpty(long[] array) {
		return array == null || (array.length == 0);
	}

	public static class Comparator<T extends Comparable<T>> {
		T first, second;
		Comparator<?> forward;

		public Comparator(T first, T second, Comparator<?> forward) {
			this.first = first;
			this.second = second;
			this.forward = forward;
		}

		public int compare() {
			int retVal = 0;
			if (first == null) {
				if (second == null) {
					if (forward != null) {
						retVal = forward.compare();
					}
				} else {
					retVal = -1;
				}
			} else {
				if (second == null) {
					retVal = 1;
				} else {
					retVal = first.compareTo(second);
					if (retVal == 0 && forward != null) {
						retVal = forward.compare();
					}
				}
			}
			return retVal;
		}
	}
	
	public static TimeZone getTimeZone(String id){
		TimeZone tz = null;
		try{
			tz = new ZoneInfo(id);
		}
		catch (Exception e) {
			logger.error("Error in reading timezone file for id : {}",id);
			tz = TimeZone.getTimeZone(id);
		}
		return tz;
	}
	
	public static class MeasureComparator{
		
		Collection<IMeasure> first;
		Collection<IMeasure> second;
		
		public MeasureComparator(Collection<IMeasure> first, Collection<IMeasure> second) {
			this.first = first;
			this.second = second;
		}
		
		public int compare() {
			boolean isFirstSubtractible = Utility.isNullOrEmpty(first) || first.iterator().next().isSubtractible();
			boolean isSecondSubtractible = Utility.isNullOrEmpty(second) || second.iterator().next().isSubtractible();
			
			if(isFirstSubtractible && !isSecondSubtractible)
				return -1;
			else if (!isFirstSubtractible && isSecondSubtractible)
				return 1;
			
			return 0;		
		}
	}

	public static class CollectionCompartor {
		Collection<?> first, second;
		CollectionCompartor forward;

		public CollectionCompartor(Collection<?> first, Collection<?> second,
				CollectionCompartor forward) {
			super();
			this.first = first;
			this.second = second;
			this.forward = forward;
		}

		public int compare() {
			int retVal = 0;
			if (Utility.isNullOrEmpty(first)) {
				if (Utility.isNullOrEmpty(second)) {
					if (forward != null) {
						retVal = forward.compare();
					}
				} else {
					retVal = -1;
				}
			} else {
				if (Utility.isNullOrEmpty(second)) {
					retVal = 1;
				} else {
					retVal = first.size() - second.size();
					if (retVal == 0 && forward != null) {
						retVal = forward.compare();
					}
				}
			}
			return retVal;
		}
	}

	public static boolean isDoubleDataType(Collection<IMeasure> measureNames) {
		if (measureNames == null || measureNames.isEmpty())
			return true;
		return isDoubleDataType(measureNames.iterator().next());
	}

	public static boolean isDoubleDataType(IMeasure measure) {
		return measure.getDataType().equals(Double.class);
	}
	
	public static boolean isStringDataType(IMeasure measure) {
		return measure.getDataType().equals(String.class);
	}

	public static boolean isByteArrayMeasure(IMeasure measure) {
		return !isDoubleDataType(measure);
	}
	
	public static double getStringSizeInBytes(int length) {
		double size = 56;
		
		if(length%4 != 0) {
			length = ((length / 4) + 1) * 4;
		}
		
		size += length*2;
		return size;
	}

	public static ByteBuffer[] createInitialisedByteBuffer(
			ArrayList<IMeasure> measures) {
		ByteBuffer[] byteBufferArray = new ByteBuffer[measures.size()];
		for (int i = 0; i < measures.size(); i++) {
			byteBufferArray[i] = measures.get(i).getBinaryFunction().init();
		}
		return byteBufferArray;
	}

	public static ByteBuffer[][] createInitialisedByteBuffer(
			ArrayList<IMeasure> measures,long[] sampleTimes) {
		ByteBuffer[][] byteBufferArray = new ByteBuffer[sampleTimes.length][measures.size()];

		for (int i = 0; i < sampleTimes.length; i++) {
			for (int j = 0; j < measures.size(); j++) {
				byteBufferArray[i][j] = measures.get(j).getBinaryFunction().init();
			}
		}
		return byteBufferArray;
	}

	public static double[] createInitialisedDoubleArray(
			ArrayList<IMeasure> measures) {
		double[] array = new double[measures.size()];
		for (int i = 0; i < measures.size(); i++) {
			array[i] = (measures.get(i)).getAggregationFunction().init();
		}
		return array;
	}

	public static double[][] createInitialisedDoubleArray(
			ArrayList<IMeasure> measures, long[] sampleTimes) {
		double[][] array = new double[sampleTimes.length][measures.size()];
		for (int i = 0; i < sampleTimes.length; i++) {
			for (int j = 0; j < measures.size(); j++) {
				array[i][j] = (measures.get(j)).getAggregationFunction().init();
			}

		}
		return array;
	}
	
	public static double[] createInitialisedDoubleArray(
			IMeasure measures, long[] sampleTimes) {
		double[] array = new double[sampleTimes.length];
			for (int j = 0; j < sampleTimes.length; j++) {
				array[j] = measures.getAggregationFunction().init();
			}
		return array;
	}

	public static byte[] byteBufferToByteArray(ByteBuffer buf) {
		java.nio.ByteBuffer buffer = buf.getJavaByteBuffer();
		byte[] bytes = new byte[buffer.limit()];
		System.arraycopy(buffer.array(), 0, bytes, 0, buffer.limit());
		return bytes;
	}

	public static ByteBuffer byteArrayToByteBuffer(byte[] bytes) {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		return buf;

	}

	public static List<Long> getAllTimeStamps(long startTime, long endTime,
			long gap) {
		List<Long> timeStamps = new ArrayList<Long>();
		for (long stamp = startTime; stamp < endTime; stamp += gap) {
			timeStamps.add(stamp);
		}
		return timeStamps;
	}

	public static boolean isCompleteTimeSeries(Set<Long> allTimeStampSet,
			long[] recordTimeStamps) {
		if (recordTimeStamps.length != allTimeStampSet.size()) {
			return false;
		}
		for (long recordTimeStamp : recordTimeStamps) {
			if (!allTimeStampSet.contains(recordTimeStamp)) {
				return false;
			}
		}
		return true;
	}

	public static void printStackTrace() {
		StackTraceElement[] stackTraceElements = Thread.currentThread()
				.getStackTrace();
		StringBuffer trace = new StringBuffer();
		for (StackTraceElement stackTraceElement : stackTraceElements) {
			trace.append("at ").append(stackTraceElement.getMethodName())
			.append("(").append(stackTraceElement.getClassName())
			.append(":").append(stackTraceElement.getLineNumber())
			.append(")").append(IoUtil.NEW_LINE);
		}

		logger.warn(trace.toString());
	}

	public static Map<IDimension, IGenericDimension> createDimensionsMap(
			Set<IGenericDimension> dims) {
		Map<IDimension, IGenericDimension> dimensionsMap = new HashMap<IDimension, IGenericDimension>();
		for (IGenericDimension iGenericDimension : dims) {
			if (iGenericDimension.isCachedDimension()) {
				dimensionsMap.put(iGenericDimension.getIDimension(),
						iGenericDimension);
			}
		}

		return dimensionsMap;
	}
	
	public static Set<IGenericDimension> getNonCachedDimension(
			Set<IGenericDimension> dims) {
		Set<IGenericDimension> returnSet = new HashSet<IGenericDimension>();
		for (IGenericDimension iGenericDimension : dims) {
			if (!iGenericDimension.isCachedDimension()) {
				returnSet.add(iGenericDimension);
			}
		}
		return returnSet;
	}

	public static Set<IDimension> getCachedDimensions(
			Set<IGenericDimension> dims) {
		Set<IDimension> returnSet = new HashSet<IDimension>();
		for (IGenericDimension iGenericDimension : dims) {
			if (!iGenericDimension.isCachedDimension()) {
				returnSet.addAll(iGenericDimension.getMappingDimensions());
			} else {
				returnSet.add(iGenericDimension.getIDimension());
			}
		}
		return returnSet;
	}

	public static Set<Set<Entry<IGenericDimension,Collection<SingleFilter>>>> powerSet(Set<Entry<IGenericDimension,Collection<SingleFilter>>> originalSet) {
        Set<Set<Entry<IGenericDimension,Collection<SingleFilter>>>> sets = new HashSet<Set<Entry<IGenericDimension,Collection<SingleFilter>>>>();
        if (originalSet.isEmpty()) {
        	sets.add(new HashSet<Entry<IGenericDimension,Collection<SingleFilter>>>());
        	return sets;
        }
        List<Entry<IGenericDimension,Collection<SingleFilter>>> list = new ArrayList<Entry<IGenericDimension,Collection<SingleFilter>>>(originalSet);
        Entry<IGenericDimension,Collection<SingleFilter>> head = list.get(0);
        Set<Entry<IGenericDimension,Collection<SingleFilter>>> rest = new HashSet<Entry<IGenericDimension,Collection<SingleFilter>>>(list.subList(1, list.size())); 
        for (Set<Entry<IGenericDimension,Collection<SingleFilter>>> set : powerSet(rest)) {
        	Set<Entry<IGenericDimension,Collection<SingleFilter>>> newSet = new HashSet<Entry<IGenericDimension,Collection<SingleFilter>>>();
        	newSet.add(head);
        	newSet.addAll(set);
        	sets.add(newSet);
        	sets.add(set);
        }		
        return sets;
    }
//	
//	public static Set<IGenericDimension> getGenericDimensions(
//			Set<IDimension> dimensions) {
//		final Map<IDimension, IGenericDimension> dimensionToGenericDimensionMap = ConfigFactory
//				.getInstance().getBean(IGenericConfig.class)
//				.getDimensionToGenericDimensionMap();
//		Set<IGenericDimension> genericDimensions = asSet(dimensions,
//				new Function<IDimension, IGenericDimension>() {
//			@Override
//			public IGenericDimension apply(IDimension input) {
//				return dimensionToGenericDimensionMap.get(input);
//			}
//
//		});
//
//		return genericDimensions;
//
//	}

//	public static String getTimeZone(Object timeZone) {
//		return timeZone != null ? timeZone.toString() : AcumeConfiguration.TimeZone.getValue();
//	}

	public static int getHour(long sampleTime, Calendar cal) {
		cal.setTimeInMillis(sampleTime*1000);
		return cal.get(Calendar.HOUR_OF_DAY);

	}

	@SuppressWarnings("unchecked")
	public static Intervals getModifiedRanges(Intervals ranges,
			Collection<Class<? extends IRule>> ruleServices) {
		if (Utility.isNullOrEmpty(ruleServices))
			return ranges;

		List<Interval> intervalList = new ArrayList<Interval>();

		Iterator<Interval> itr = ranges.iterator();
		while (itr.hasNext()) {
			Interval interval = itr.next();

			Set<Integer> timestamps = new HashSet<Integer>();
			for (Class<? extends IRule> service : ruleServices) {
				timestamps.addAll(TimeBasedRuleServiceUtility
						.getChangeTimestamp(service,
								(int) interval.getStartTime(),
								(int) interval.getEndTime(), null));
			}

			List<Integer> timestampsList = new ArrayList<Integer>(timestamps);
			if (interval.getStartTime() == interval.getPresentTime())
				addForwardIntervalList(interval, ruleServices, timestampsList,
						intervalList);
			else
				addBackwardIntervalList(interval, ruleServices, timestampsList,
						intervalList);
		}

		Interval[] intervalArr = new Interval[intervalList.size()];
		Intervals intervals = new Intervals(intervalList.toArray(intervalArr));

		return intervals;
	}

	private static void addForwardIntervalList(Interval interval,
			Collection<Class<? extends IRule>> ruleServices,
			List<Integer> timestampsList, List<Interval> intervalList) {
		Collections.sort(timestampsList);

		int start = (int) interval.getStartTime();
		int end;

		// add intervals from rulesServices
		if (timestampsList != null) {
			for (Integer timestamp : timestampsList) {
				end = timestamp;
				Interval range = new Interval(start, end);
				intervalList.add(range);
				start = timestamp;
			}
		}

		end = (int) interval.getEndTime();
		Interval range = new Interval(start, end);
		intervalList.add(range);
	}

	private static void addBackwardIntervalList(Interval interval,
			Collection<Class<? extends IRule>> ruleServices,
			List<Integer> timestampsList, List<Interval> intervalList) {
		Collections.sort(timestampsList, Collections.reverseOrder());

		int end = (int) interval.getEndTime();
		int start;

		// add intervals from rulesServices
		if (timestampsList != null) {
			for (Integer timestamp : timestampsList) {
				start = timestamp;
				Interval range = new Interval(start, end, end);
				intervalList.add(range);
				end = timestamp;
			}
		}

		start = (int) interval.getStartTime();
		Interval range = new Interval(start, end, end);
		intervalList.add(range);
	}

	public static int getFraction(String timeZone) {
		String[] splitTimeZone = timeZone.split(":");
		int fractionTime = Integer
				.parseInt(splitTimeZone[splitTimeZone.length - 1]);
		return fractionTime * 60;
	}

	public static int getTimeInSecUTC(int hourOfDay) {
		Calendar cal = newUTCCalendar();
		cal.setTimeInMillis(0);
		cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
		return (int) (cal.getTimeInMillis() / 1000);
	}

	public static LinkedHashSet<IMeasure> getIMeasures(List<FilterOperation> filters) {
		LinkedHashSet<IMeasure> mList = new LinkedHashSet<IMeasure>();
		if (filters != null) {
			for (FilterOperation filter : filters) {
				mList.add(filter.getMeasure());
			}
		}
		return mList;
	}

	public static Double mergeDoubles(INumericFunction function, 
			Collection<Double> values, Collection<Integer> dimensionValues, Collection<IDimension> dimensions) {
		Double result = 0.0;
		for (Double val : values) {
			result = function.compute(result, val, new AggregationContext(dimensionValues, dimensions, DataMergeType.HORIZONTAL));
		}
		return result;
	}
	
	public static ByteBuffer[] computeAll(List<IMeasure> measureNames, final List<ByteBuffer[]> allTupleMeasures, int[] measureIndices, 
			DataMergeType mergeType, byte[] emptyBuffersPosition, Long granularity, ArrayList<Integer> compDimFilterValues) {
		ByteBuffer[] resultMeasures = new ByteBuffer[measureIndices.length];
		for (int k = 0; k < measureIndices.length; k++) {
			_computeAll(allTupleMeasures, resultMeasures, measureNames, measureIndices[k], k, mergeType, emptyBuffersPosition, granularity, compDimFilterValues);
		}
		return resultMeasures;
	}

	private static void _computeAll(final List<ByteBuffer[]> allTupleMeasures,
			ByteBuffer[] resultMeasures, List<IMeasure> measureNames, final int index,
			final int j, DataMergeType mergeType, final byte[] emptyBuffersPosition, Long granularity, ArrayList<Integer> compDimFilterValues) {
	    ArrayList<java.nio.ByteBuffer> tempBufferList = new ArrayList<java.nio.ByteBuffer>() {
            @Override
            public java.nio.ByteBuffer get(int index1) {
                return allTupleMeasures.get(index1)[index].getJavaByteBuffer();
            }
            
            @Override
            public int size() {
                return allTupleMeasures.size();
            }
            
            @Override
            public Iterator<java.nio.ByteBuffer> iterator() {
                return new ByteBufferIterator(index, allTupleMeasures);
            }
        };
        tempBufferList.size();
		switch (mergeType) {
		case HORIZONTAL:
			resultMeasures[j] = measureNames.get(j).getBinaryFunction().computeAllHorizontal(tempBufferList, emptyBuffersPosition, granularity, compDimFilterValues);
			break;
		case VERTICAL:
			resultMeasures[j] = measureNames.get(j).getBinaryFunction().computeAllVertical(tempBufferList, compDimFilterValues);
			break;
		default:
			throw new IllegalArgumentException("Unsupported merge type!");
		}
	}
	
	
	public static TimeZoneInfo getTimeZoneInfo(String id, int startYear, int endYear) throws Exception{
		int[] transTimes;
		byte[] transTypes;
		byte[] dst;
		int[] offset;
		byte[] idx;
		int utcOffset = 0;
		String[] tzname;
		File f = new File(RubixProperties.TimeZoneDBPath.getValue(),id);
		DataInputStream ds = new DataInputStream(new BufferedInputStream(
				new FileInputStream(f)));

		try {
			ds.skip(32);
			int timecnt = ds.readInt();
			int typecnt = ds.readInt();
			int charcnt = ds.readInt();
			transTimes = new int[timecnt];
			for (int i = 0; i < timecnt; ++i){
				transTimes[i] = ds.readInt();
			}
			transTypes = new byte[timecnt];
			ds.readFully(transTypes);

			offset = new int[typecnt];
			dst = new byte[typecnt];
			idx = new byte[typecnt];
			for (int i = 0; i < typecnt; ++i) {
				offset[i] = ds.readInt();
				dst[i] = ds.readByte();
				idx[i] = ds.readByte();
			}
			byte[] str = new byte[charcnt];
			ds.readFully(str);
			tzname = new String[typecnt];
			for (int i = 0; i < typecnt; ++i) {
				int pos = idx[i];
				int end = pos;
				while (str[end] != 0) ++end;
				tzname[i] = new String(str,pos,end-pos);
			}
			for(int i = transTimes.length - 1 ; i > 0 ; i--){
				if(dst[transTypes[i]] == 0){
					utcOffset = offset[transTypes[i]];
					break;
				}
			}
		}
		finally {
			ds.close(); 
		}
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		List<List<String>> rules = new ArrayList<List<String>>();
				for(int i = 0 ; i < transTimes.length ; i++){
					if(i > 0){
						cal.setTimeInMillis(transTimes[i] * 1000L + offset[transTypes[i-1]] * 1000L);
					}
					else{
						cal.setTimeInMillis(transTimes[i] * 1000L + utcOffset);
					}
					int year = cal.get(Calendar.YEAR);
					if(year < startYear || year > endYear) continue;
					List<String> tempRule = new ArrayList<String>();
					tempRule.add(String.valueOf(year));
					tempRule.add(String.valueOf(cal.get(Calendar.DAY_OF_WEEK_IN_MONTH)));
					tempRule.add(String.valueOf(cal.get(Calendar.DAY_OF_WEEK)));
					tempRule.add(String.valueOf(cal.get(Calendar.MONTH)));
					tempRule.add(String.valueOf(cal.get(Calendar.HOUR_OF_DAY)));
					tempRule.add(String.valueOf(cal.get(Calendar.MINUTE)));
					tempRule.add(String.valueOf((offset[transTypes[i]] - utcOffset )));
					tempRule.add(String.valueOf(utcOffset));
					tempRule.add(tzname[transTypes[i]]);
					rules.add(tempRule);
				}
		TimeZone zone = TimeZone.getTimeZone(id);
		String zoneName = zone.getDisplayName(false, 0);
		String zoneFullName = zone.getDisplayName(false, 1);
		String dstName = zone.getDisplayName(true, 0);
		String dstFullName = zone.getDisplayName(true, 1);
		TimeZoneInfo result = new TimeZoneInfo(rules, utcOffset, id, zoneName, zoneFullName, dstName, dstFullName);
		return result;
	}
	
	public static List<TimeZoneInfo> getTimeZoneInfo(List<String> ids , int startYear, int endYear) throws Exception{
		List<TimeZoneInfo> result = new ArrayList<TimeZoneInfo>();
		for(String id:ids){
			result.add(getTimeZoneInfo(id, startYear, endYear));
		}
		return result;
	}
	
    public static String getTimeInHumanReadableForm(long time, String timeZone) {
        Calendar calendar = Calendar.getInstance(Utility.getTimeZone(timeZone));
        calendar.setTimeInMillis(time * 1000);
        SimpleDateFormat formatter = new SimpleDateFormat("MMM dd EEE yyyy HH:mm z");
        return formatter.format(calendar.getTime());
    }
    
    public static String getCurrentDateInHumanReadableForm() {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");
        return formatter.format(calendar.getTime());
    }
    
    public static void closeStream(Closeable s) {
        try{
            if(s!=null)s.close();
        }catch(IOException e){
            throw new RuntimeException("Unable to close stream ", e);
        }
    }
	
	public static int daysBetween(long rangeStartTime, long rangeEndTime, Calendar instance) 
	{
		rangeStartTime = Utility.ceilingToTimezone(rangeStartTime, TimeGranularity.DAY, instance);
		rangeEndTime = Utility.floorToTimezone(rangeEndTime, TimeGranularity.DAY, instance);
		if (rangeEndTime == rangeStartTime) 
		{
			return 0;
		}
		instance.setTimeInMillis(rangeEndTime * 1000);
		int daysBetween = 0;
		for(;Utility.getNextTimeFromGranularity(rangeStartTime, TimeGranularity.DAY.getGranularity(), instance)<= rangeEndTime;rangeStartTime = Utility.getNextTimeFromGranularity(rangeStartTime, TimeGranularity.DAY.getGranularity(), instance)) 
		{
			daysBetween++;
		}

		return daysBetween;
	}

//    private static <N,M> Map<Integer, M> operation(Operation operation,
//    		Map<Integer, M> resultTupleMeasureMap,
//    		Map<Integer, N> sourceTupleMeasureMap, Collection<IMeasure> measureNames) {
//    	return operation.apply(resultTupleMeasureMap, sourceTupleMeasureMap, measureNames);
//    }
//    
//    private static <N,M> Map<Integer, M> operationMulti(OperationMulti operation,
//    		List<Map<Integer, N>> tupleMaps, int[] measureIndices, Map<Integer, ArrayList<Integer>> tupleIds,
//    		List<IMeasure> measures, Long granularity, DataMergeType mergeType, Integer segmentId, RubixCache<?, ?> cache) {
//    	return operation.apply(tupleMaps, measureIndices, tupleIds, measures, granularity,mergeType, segmentId, cache);
//    	
//    }
    
    public static LinkedHashSet<IDimension> getNonStaticKeys(ICube profileCube){
    	PrefetchConfiguration prefetchConfiguration = profileCube.getPrefetchConfiguration();
    	if(prefetchConfiguration != null){
    		if(Utility.isNullOrEmpty(prefetchConfiguration.getKeys()))
    			return profileCube.getKeys();
    		else {
    			List<Map<IDimension, Integer>> prefecthKeys = prefetchConfiguration.getKeys();
    			Set<IDimension> staticKeys = prefecthKeys.iterator().next().keySet();
    			LinkedHashSet<IDimension> profileCubeKeys = Sets.newLinkedHashSet(profileCube.getKeys());
    			profileCubeKeys.removeAll(staticKeys);
    			return profileCubeKeys;
    		}
    	}
    	
    	return profileCube.getKeys();
    }

	public static int getSubscriberValidityTimeRange(){
		return RubixProperties.MinimumSubscriberCountDays.getIntValue()*24*60*60;
	}
	
	public static Double roundMeasureValue(Double measureValue) {
        return measureValue.equals(0D) ? measureValue :
                (measureValue < 1) ? 1 : Math.round(measureValue);
    }

	public static void main(String[] args) {
		Calendar instance = Calendar.getInstance(TimeZone
				.getTimeZone("GMT+5:30"));
		System.out.println(((double)33 )/7);
		System.out.println(roundDoubleValue(((double)33/7), 5));
		
		System.out.println(Math.min(Double.NaN, -1));
		
		long time = 1362854800;
		
		instance.setTimeInMillis(time*1000);
		System.out.println("original time" + instance.getTime());
		
		long three_hour_ceil = ceilingToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.THREE_HOUR);
		instance.setTimeInMillis(three_hour_ceil*1000);
		System.out.println("three_hour_ceil "+ instance.getTime());
		
		long three_hour_floor = floorToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.THREE_HOUR);
		instance.setTimeInMillis(three_hour_floor*1000);
		System.out.println("three_hour_floor "+ instance.getTime());
		
		long fifteen_minute_ceil = ceilingToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.FIFTEEN_MINUTE);
		instance.setTimeInMillis(fifteen_minute_ceil*1000);
		System.out.println("fifteen_minute_ceil "+ instance.getTime());
		
		
		long fifteen_minute_floor = floorToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.FIFTEEN_MINUTE);
		instance.setTimeInMillis(fifteen_minute_floor*1000);
		System.out.println("fifteen_minute_floor "+ instance.getTime());
		
		long one_minute_ceil = ceilingToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.ONE_MINUTE);
		instance.setTimeInMillis(one_minute_ceil*1000);
		System.out.println("one_minute_ceil "+ instance.getTime());
		
		
		long one_minute_floor = floorToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.ONE_MINUTE);
		instance.setTimeInMillis(one_minute_floor*1000);
		System.out.println("one_minute_floor "+ instance.getTime());
		
		long two_day_ceil = ceilingToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.TWO_DAYS);
		instance.setTimeInMillis(two_day_ceil*1000);
		System.out.println("two_day_ceil "+ instance.getTime());
		
		long two_day_floor = floorToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.TWO_DAYS);
		instance.setTimeInMillis(two_day_floor*1000);
		System.out.println("two_day_floor "+ instance.getTime());
		
		long week_floor = floorToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.WEEK);
		instance.setTimeInMillis(week_floor*1000);
		System.out.println("week_floor "+ instance.getTime());
		
		long week_ceil = ceilingToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.WEEK);
		instance.setTimeInMillis(week_ceil*1000);
		System.out.println("week_ceil "+ instance.getTime());
		
		
		long three_day_ceil = ceilingToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.THREE_DAYS);
		instance.setTimeInMillis(three_day_ceil*1000);
		System.out.println("three_day_ceil "+ instance.getTime());
		
		long three_day_floor = floorToTimeZone(time, Utility.getTimeZone("GMT+5:30"), TimeGranularity.THREE_DAYS);
		instance.setTimeInMillis(three_day_floor*1000);
		System.out.println("three_day_floor "+ instance.getTime());

	}
	
	public static boolean isCause(Class<? extends Throwable> expected,
			Throwable exc) {
		return expected.isInstance(exc)
				|| (exc != null && isCause(expected, exc.getCause()));
	}

	public static void throwIfRubixException(Throwable t){
		Iterator<RubixException> reItr = Iterables.filter(Utility.getCausalChain(t), RubixException.class).iterator();
		if(reItr.hasNext())
			throw reItr.next();
	}
	
	public static TopologyMismatchException getTopologyMismatchException(Throwable t){
		Iterator<TopologyMismatchException> reItr = Iterables.filter(Utility.getCausalChain(t), TopologyMismatchException.class).iterator();
		if(reItr.hasNext()){
			return reItr.next();
		}
		return null;
	}
	
	
	public static RubixException getRubixException(Throwable t){
		Iterator<RubixException> reItr = Iterables.filter(Utility.getCausalChain(t), RubixException.class).iterator();
		if(reItr.hasNext()){
			return reItr.next();
		}
		return null;
	}
	
	public static LinkedHashSet<Throwable> getCausalChain(Throwable throwable) {
	    
		LinkedHashSet<Throwable> causes = new LinkedHashSet<Throwable>();
	    while (throwable != null && !causes.contains(throwable)) {
	      causes.add(throwable);
	      throwable = throwable.getCause();
	    }
	    return causes;
	  }
	
	public static long floorFromGranularity(long time, long gran){
		TimeGranularity timeGranularity = TimeGranularity.getTimeGranularity(gran);
		return floorToTimeZone(time, timeGranularity);
	}
	
	public static long floorFromGranularityAndTimeZone(long time, long gran, TimeZone timezone){
		if(timezone!= null){
			TimeGranularity timeGranularity = TimeGranularity.getTimeGranularity(gran);
			Calendar instance = newCalendar(timezone);
			return floorToTimezone(time, timeGranularity, instance);
		}
		else{
			return floorFromGranularity(time, gran);
		}
	}

	public static long getPreviousTimeForGranularity(long time, long gran, Calendar instance){
		TimeGranularity timeGranularity = TimeGranularity.getTimeGranularity(gran);
		return floorToTimezone(time - getMinTimeGran(gran), timeGranularity, instance);
	}
	
	public static long ceilingFromGranularity(long time, long gran){
		TimeGranularity timeGranularity = TimeGranularity.getTimeGranularity(gran);
		return ceilingToTimeZone(time, timeGranularity);
	}
	
	public static long getMinTimeGran(long gran) {
		if(gran == TimeGranularity.HOUR.getGranularity()) {
			return TimeGranularity.HOUR.getGranularity();
		}
		return TimeGranularity.ONE_MINUTE.getGranularity();
	}
	public static long getNextTimeFromGranularity(long time, long gran, Calendar instance){
		TimeGranularity timeGranularity = TimeGranularity.getTimeGranularity(gran);
		return ceilingToTimezone(time + getMinTimeGran(gran), timeGranularity, instance);
	}
	
	public static long getStartTimeFromLevel(long endTime,long granularity, int points) {
			long rangeEndTime = Utility.floorFromGranularity(endTime, granularity);
			long rangeStartTime ;
			Calendar cal = Utility.newCalendar();
			if(granularity == TimeGranularity.MONTH.getGranularity()){
				cal.setTimeInMillis(rangeEndTime * 1000);
				cal.add(Calendar.MONTH, -1 * points);
				rangeStartTime = cal.getTimeInMillis() / 1000;
			}
			else if(granularity == TimeGranularity.WEEK.getGranularity()) {
				cal.setTimeInMillis(rangeEndTime * 1000);
				cal.add(Calendar.WEEK_OF_MONTH, -1 * points);
				rangeStartTime = cal.getTimeInMillis() / 1000;
			}else if(granularity == TimeGranularity.DAY.getGranularity()) {
				cal.setTimeInMillis(rangeEndTime * 1000);
				cal.add(Calendar.DAY_OF_MONTH, -1 * points);
				rangeStartTime = cal.getTimeInMillis() / 1000;
			}
			else{
				rangeStartTime = rangeEndTime - points*granularity;
			}
			return rangeStartTime;
	}
	
	/**
	 * Function will return the combined current memory snapshot value for the selected threads
	 */
	public static long getCurrentMemoryReading() {
		com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
		java.lang.management.ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 0);
		long mem = 0;
		for (java.lang.management.ThreadInfo threadInfo : threadInfos) {
			for (String validThreadPattern: validThreadPatternNames) {
				if (threadInfo.getThreadName().startsWith(validThreadPattern)) {
					mem += threadMXBean.getThreadAllocatedBytes(threadInfo.getThreadId());
					break;
				}
			}
		}
		return mem;
	}
	
	public static long getEndTimeFromLevel(long startTime,long granularity, int points) {
		long rangeStartTime = Utility.ceilingFromGranularity(startTime, granularity);
		long rangeEndTime ;
		Calendar cal = Utility.newCalendar();
		if(granularity == TimeGranularity.MONTH.getGranularity()){
			cal.setTimeInMillis(rangeStartTime * 1000);
			cal.add(Calendar.MONTH, 1 * points);
			rangeEndTime = cal.getTimeInMillis() / 1000;
		}
		else if(granularity == TimeGranularity.WEEK.getGranularity()) {
			cal.setTimeInMillis(rangeStartTime * 1000);
			cal.add(Calendar.WEEK_OF_MONTH, 1 * points);
			rangeEndTime = cal.getTimeInMillis() / 1000;
		}else if(granularity == TimeGranularity.DAY.getGranularity()) {
			cal.setTimeInMillis(rangeStartTime * 1000);
			cal.add(Calendar.DAY_OF_MONTH, 1 * points);
			rangeEndTime = cal.getTimeInMillis() / 1000;
		}
		else{
			rangeEndTime = rangeStartTime + points*granularity;
		}
		return rangeEndTime;
}

	
	public static List<Long> getAllIntervals(long startTime, long endTime, long gran){
		List<Long> intervals = new LinkedList<Long>();
		Calendar instance = Utility.newCalendar();
		while(startTime < endTime) {
			intervals.add(startTime);
			startTime = Utility.getNextTimeFromGranularity(startTime, gran, instance);
		}
		return intervals;
	}
	
	public static List<Long> getAllIntervalsAtTZ(long startTime, long endTime, long gran, TimeZone timezone){
		if(timezone == null) return getAllIntervals(startTime, endTime, gran);
		List<Long> intervals = new LinkedList<Long>();
		Calendar instance = newCalendar(timezone);
		while(startTime < endTime) {
			intervals.add(startTime);
			startTime = Utility.getNextTimeFromGranularity(startTime, gran, instance);
		}
		return intervals;
	}

	public static Double[] getDoubleArray(Object val) {
		if (val instanceof double[]) {
			double[] mVals = (double[]) val;
			return ArrayUtils.toObject(mVals);
		} else {
			return (Double[]) val;
		}
	}
	
	public static double roundDoubleValue(double input, int noOfDecimalPlaces) {
		double factor = Math.pow(10, noOfDecimalPlaces);
		return Math.round(input * factor) / factor;
	}
	
	
	public static SortedMap<Long,Integer> getLevelPointMap(String mapString) throws IllegalArgumentException{
		SortedMap<Long,Integer> result = new TreeMap<Long, Integer>();
		StringTokenizer tok = new StringTokenizer(mapString, ";");
		while(tok.hasMoreTokens()){
			String currentMapElement = tok.nextToken();
			String gran;
			int points;
			try{
				gran = currentMapElement.substring(
						0, currentMapElement.indexOf(':'));
				points = Integer.valueOf(currentMapElement.substring(currentMapElement.indexOf(':')+1));
			}
			catch (Exception e) {
				throw new IllegalArgumentException("Malformed VariableRetentionMap String");
			}
			TimeGranularity granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(gran);
			if (granularity == null) {
				throw new IllegalArgumentException("Unsupported Granularity  "+gran);
			}
			long level = granularity.getGranularity();
			result.put(level, points);
		}
		return result;
	}

	public static SortedMap<Long,Integer> getDiskPersistLevelPointMap(String mapString) throws IllegalArgumentException{
		SortedMap<Long,Integer> result = new TreeMap<Long, Integer>();
		StringTokenizer tok = new StringTokenizer(mapString, ";");
		while(tok.hasMoreTokens()){
			String currentMapElement = tok.nextToken();
			String gran;
			int points;
			try{
				gran = currentMapElement.substring(
						0, currentMapElement.indexOf(':'));
				points = Integer.valueOf(currentMapElement.substring(currentMapElement.indexOf(':')+1));
			}
			catch (Exception e) {
				throw new IllegalArgumentException("Malformed DiskPersistLevelPointMap String");
			}
			TimeGranularity granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(gran);
			if (granularity == null) {
				throw new IllegalArgumentException("Unsupported Granularity  "+gran);
			}
			long level = granularity.getGranularity();
			
			if(RubixProperties.BinReplay.getBooleanValue() && points > 1){
				logger.warn("BinReplay is not supported for diskCombinePoints>1. hence setting combine points as 1");
				points=1;
			}
			
			if(!TimeGranFinder.validateLevelAndPoints(level, points)){
				throw new IllegalArgumentException("Wrong numPoints configured("+points+") for gran  "+gran);
			}
			result.put(level, points);
		}
		return result;
	}
	
	public static <M> void removeTuplesWithAllZeroMeasures(
			Map<Integer, Map<Integer, M>> inputTupleMeasureMap) {
		Set<Integer> toBeRemovedSegmentIds = new HashSet<Integer>();
		for(Entry<Integer, Map<Integer, M>> entry : inputTupleMeasureMap.entrySet()) {
			Iterator<Integer> itr = entry.getValue().keySet().iterator();
	        while (itr.hasNext()) {
	            if(isAllZero((double[])entry.getValue().get(itr.next()))) 
	            	itr.remove();
	        }
	        if(entry.getValue().keySet().isEmpty()) {
	        	toBeRemovedSegmentIds.add(entry.getKey());
	        }
		}
		for(Integer segmentId : toBeRemovedSegmentIds) {
			inputTupleMeasureMap.remove(segmentId);
		}
	}
	
	public static void trimByteBuffer(ByteBuffer[] buffer, Collection<IMeasure> measures){
		Iterator<IMeasure> iterator = measures.iterator();
		for (int i = 0; i < buffer.length; i++) {
			IMeasure measure = iterator.next();
			if (!measure.getBinaryFunction().isEmpty(buffer[i])) {
				measure.getBinaryFunction().convert(buffer[i]);
				int bufferWidth = measure.getBinaryFunction()
						.getRequiredByteBufferWidth(buffer[i]);
				if (bufferWidth != measure.getBinaryFunction().size()) {
					ByteBuffer copyByteBuffer = copyByteBuffer(
							buffer[i], bufferWidth);
					buffer[i] = copyByteBuffer;
				}
			}
		}
	}
	
	public static boolean isAllZero(double[] doubles) {
        for (double d : doubles) {
            if (d != 0.0) {
                return false;
            }
        }
    	return true;
    }
	
	private static ByteBuffer copyByteBuffer(ByteBuffer byteBuffer , int limit) {
    	byte[] bytes = new byte[limit];
    	System.arraycopy(byteBuffer.array(), 0, bytes, 0, limit);
    	return ByteBuffer.wrap(bytes);
    }
	
	public static Calendar newCalendar(){
		return (Calendar)calendar.clone();
	}
	
	public static Calendar newCalendar(TimeZone timezone){
		return Calendar.getInstance(timezone);
	}
	
	public static Calendar newUTCCalendar(){
		return (Calendar)utcCalendar.clone();
	}

	public static void rollbackIfStillOpen(Session session) {
        if (session.isOpen() && !session.getTransaction()
            .wasCommitted()) {
            session.getTransaction()
                .rollback();
        }
    }
	
	public static Set<IMeasure> getDependentMeasures(IMeasure measure, IMeasureProcessorMap measureProcessorMap){
		Set<IMeasure> allMeasures = new HashSet<IMeasure>();
		allMeasures.add(measure);
		return getDependentMeasures(allMeasures, measureProcessorMap);
	}
	
	public static void freeDirectBuffer(DirectBuffer buffer) {
		Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
	    if (cleaner != null) cleaner.clean();
	}
	
	public static Set<IMeasure> getDependentMeasures(Set<IMeasure> allMeasures, IMeasureProcessorMap measureProcessorMap){
		Set<IMeasure> result = new HashSet<IMeasure>();
		for(IMeasure measure : allMeasures){
			IMeasureProcessor measureProcessor = measureProcessorMap.getProcessor(measure);
			if(measureProcessor!=null){
				Set<IMeasure> dependentMeasures = new HashSet<IMeasure>();
				IRequest req = new Request();
				Collection<IRequest> dependentRequest;
				try{
					dependentRequest = measureProcessor.getDependentDataRequest(req, measureProcessorMap);
				}
				catch (Exception e) {
					dependentRequest = new HashSet<IRequest>();
				}
				if(dependentRequest!=null){
					for(IRequest request : dependentRequest){
						List<IMeasure> measures = request.getMeasures();
						if(measures !=null)
							dependentMeasures.addAll(measures);
					}
				}
				List<IMeasure> measures = measureProcessor.getDependentMeasures();
				if(measures != null)
					dependentMeasures.addAll(measures);
				if(measureProcessor instanceof IDependentMeasuresContainer){
					List<IMeasure> allDependentMeasures = ((IDependentMeasuresContainer)measureProcessor).getAllDependentMeasures();
					if(allDependentMeasures!=null){
						dependentMeasures.addAll(allDependentMeasures);
					}
				}
				result.addAll(getDependentMeasures(dependentMeasures, measureProcessorMap));
			}
			else{
				result.add(measure);
			}
		}
		return result;
	}
	
	public static Iterable<java.nio.ByteBuffer> getIterable(final Collection<ByteBuffer> byteBuffers) {
		return new Iterable<java.nio.ByteBuffer>() {
			@Override
			public Iterator<java.nio.ByteBuffer> iterator() {
				return new SingleByteBufferIterator(byteBuffers);
			}
		};
	}
	 
    /**
     * Copies passed inputstream to passed outputstream.
     * @param input
     * @param output
     * @throws IOException
     */
    public static void copyStream(InputStream input, OutputStream output)    
            throws IOException {
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = input.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
    }
    }

class ByteBufferIterator implements Iterator<java.nio.ByteBuffer> {

	private Iterator<ByteBuffer[]> iterator;
	private int k;

	public ByteBufferIterator(int k, Iterable<ByteBuffer[]> iterable) {
		this.iterator = iterable.iterator();
		this.k = k;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public java.nio.ByteBuffer next() {
		return iterator.next()[k].getJavaByteBuffer();
	}

	@Override
	public void remove() {
		throw new IllegalStateException();
	}
}

class SingleByteBufferIterator implements Iterator<java.nio.ByteBuffer> {

	private Iterator<ByteBuffer> iterator;

	public SingleByteBufferIterator(Iterable<ByteBuffer> iterable) {
		this.iterator = iterable.iterator();
	}

	
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	
	@Override
	public java.nio.ByteBuffer next() {
		return iterator.next().getJavaByteBuffer();
	}

	@Override
	public void remove() {
		throw new IllegalStateException();
	}
}
