package com.guavus.acume.util;

import static com.guavus.acume.core.MeasureType$.MODULE$;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.shiro.SecurityUtils;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rubix.exception.RubixException;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;
import util.io.IoUtil;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.guavus.bloomfilter.BloomFilterOperatorStaticFactory;
import com.guavus.bloomfilter.IBloomFilterOperator;
import com.guavus.rubix.cache.CacheType;
import com.guavus.rubix.cache.DataStore;
import com.guavus.rubix.cache.Interval;
import com.guavus.rubix.cache.Intervals;
import com.guavus.rubix.cache.RubixCache;
import com.guavus.rubix.cache.TimeGranFinder;
import com.guavus.rubix.cache.TimeGranularity;
import com.guavus.rubix.cds.AggrTableImpl;
import com.guavus.rubix.cds.ByteBuffer;
import com.guavus.rubix.cds.Table;
import com.guavus.rubix.cds.TimeseriesTableImpl;
import com.guavus.rubix.configuration.BinSource;
import com.guavus.rubix.configuration.ConfigFactory;
import com.guavus.rubix.configuration.IGenericConfig;
import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.AggregationContext;
import com.guavus.rubix.core.Controller;
import com.guavus.rubix.core.DataMergeType;
import com.guavus.rubix.core.ICacheType;
import com.guavus.rubix.core.ICube;
import com.guavus.rubix.core.IDimension;
import com.guavus.rubix.core.IMeasure;
import com.guavus.rubix.core.INumericFunction;
import com.guavus.rubix.core.IRecord;
import com.guavus.rubix.core.PrefetchConfiguration;
import com.guavus.rubix.core.distribution.TopologyMismatchException;
import com.guavus.rubix.distributed.query.DataServiceUtil;
import com.guavus.rubix.filter.FilterOperation;
import com.guavus.rubix.parameters.FilterObject.SingleFilter;
import com.guavus.rubix.parameters.FilterRequest;
import com.guavus.rubix.query.IGenericDimension;
import com.guavus.rubix.query.IQueryRequest;
import com.guavus.rubix.query.QueryRequestMode;
import com.guavus.rubix.query.SubQueryFilter;
import com.guavus.rubix.query.remote.flex.TimeZoneInfo;
import com.guavus.rubix.rules.IRule;
import com.guavus.rubix.rules.TimeBasedRuleServiceUtility;
import com.guavus.rubix.search.Operator;
import com.guavus.rubix.search.SearchCriterion;
import com.guavus.rubix.transform.AggregatedData;
import com.guavus.rubix.transform.TransformationEngine;
import com.guavus.rubix.workflow.Flow;
import com.guavus.rubix.workflow.IDependentMeasuresContainer;
import com.guavus.rubix.workflow.IMeasureProcessor;
import com.guavus.rubix.workflow.IMeasureProcessorMap;
import com.guavus.rubix.workflow.IRequest;
import com.guavus.rubix.workflow.IndexedTable;
import com.guavus.rubix.workflow.Request;
import com.guavus.rubix.workflow.RequestDataType;

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
    
    public static boolean isTreeCache(ICube cube) {
    	
    	if(cube.isAggregate() && (cube.getOutputMeasures().isEmpty() || cube.getOutputMeasures().iterator().next().isSubtractible())) {
    		return false;
    	}
    	return true;
    }

	
    /**
     * Method to differentiate between query from scheduler or UI.
     *
     * @return false if query is from scheduler true if query from UI.
     */
    public static boolean isSchedulerQuery(QueryRequestMode reqMode) {
        return reqMode.equals(QueryRequestMode.SCHEDULER);
    }
    
	public static Function<double[], String> doubleArrayToStringFunction = new Function<double[], String>() {
		@Override
		public String apply(double[] from) {
			return Arrays.toString(from);
		}

	};

	public static Function<int[], String> intArrayToStringFunction = new Function<int[], String>() {
		@Override
		public String apply(int[] from) {
			return Arrays.toString(from);
		}

	};

	public static Function<Object, String> identityFunction = new Function<Object, String>() {
		@Override
		public String apply(Object from) {
			return from != null ? from.toString() : null;
		}

	};

	public static <E, T> Iterator<T> iterator(Collection<E> collection,
			Function<E, Iterator<T>> function) {
		Iterator<T> itr = null;
		for (E e : collection) {
			if (itr == null) {
				itr = function.apply(e);
			} else {
				itr = Iterators.concat(itr, function.apply(e));
			}
		}
		return itr;
	}

	public static <E, T> LinkedHashSet<T> asLinkedHashSet(
			Collection<E> collection, Function<E, Collection<T>> function) {
		LinkedHashSet<T> set = new LinkedHashSet<T>();
		for (E e : collection) {
			set.addAll(function.apply(e));
		}
		return set;
	}

	public static <E, T> Set<T> asSet(
			Collection<E> collection, Function<E, T> function) {
		Set<T> set = new HashSet<T>();
		for (E e : collection) {
			set.add(function.apply(e));
		}
		return set;
	}
	
	public static <E, T, Z> Map<T, Z> asMap(
			Collection<E> collection, Function<E, Map.Entry<T, Z>> function) {
		Map<T, Z> map = new HashMap<T, Z>();
		for (E e : collection) {
			Map.Entry<T, Z> entry = function.apply(e);
			map.put(entry.getKey(), entry.getValue());
		}
		return map;
	}

	public static <T> LinkedHashSet<T> asLinkedHashSet(
			Collection<T>... collections) {
		LinkedHashSet<T> set = new LinkedHashSet<T>();
		for (Collection<T> e : collections) {
			set.addAll(e);
		}
		return set;
	}

	public static <T> LinkedHashSet<T> asLinkedHashSet() {
		return new LinkedHashSet<T>();
	}

	public static <T> LinkedHashSet<T> asLinkedHashSet(T... collection) {
		LinkedHashSet<T> set = new LinkedHashSet<T>();
		for (T e : collection) {
			set.add(e);
		}
		return set;
	}
	    
    public static long[] mergeSampleTimes(long[] sampleTimes1, long[] sampleTimes2) {
    	List<Long> samples = new ArrayList<Long>();
    	int j=0;
    	int i=0;
    	for(i=0;i<sampleTimes1.length && j<sampleTimes2.length;){
    		if(sampleTimes1[i] == sampleTimes2[j]) {
    			samples.add(sampleTimes1[i]);
    			i++;j++;
    		} else if(sampleTimes1[i]>sampleTimes2[j]) {
    			samples.add(sampleTimes2[j]);
    			j++;
    		} else {
    			samples.add(sampleTimes1[i]);
    			i++;
    		}
    	}
    	
    	if (i==sampleTimes1.length && j!=sampleTimes2.length) {
    		for(;j<sampleTimes2.length;j++) {
    			samples.add(sampleTimes2[j]);
    		}
    	} else if (i!=sampleTimes1.length && j==sampleTimes2.length) {
    		for(;i<sampleTimes1.length;i++) {
    			samples.add(sampleTimes1[i]);
    		}
    	}
    	long[] returnedSamples = new long[samples.size()];
    	int k=0;
    	for (Long sample : samples) {
			returnedSamples[k] = sample;
					k++;
		}
    	return returnedSamples;
    }
	    
	public static <A, F> String toString(Map<A, F> tuples, int maxLen) {
		return toString(tuples, maxLen, identityFunction);
	}
	
	/**
	 * Returns the bytebuffer based upon the datastore heap or offheap
	 * @param size
	 * @param store
	 * @return
	 */
	public static ByteBuffer getByteBuffer(int size, DataStore store) {
		if(store == DataStore.HEAP) {
			return new ByteBuffer(java.nio.ByteBuffer.allocate(size));
		} else if(store == DataStore.OS){
			return new ByteBuffer(java.nio.ByteBuffer.allocateDirect(size));
		}
		else throw new IllegalArgumentException("Writing to store : "+store +" is not supported");
	}

	public static <A, F, T extends F> String toString(Map<A, T> map,
			int maxLen, final Function<F, String> f) {
		if (map == null)
			return null;
		return toString(map.entrySet(), maxLen,
				new Function<Map.Entry<A, T>, String>() {

			@Override
			public String apply(Entry<A, T> from) {
				StringBuilder builder = new StringBuilder();
				builder.append(from.getKey());
				builder.append("=");
				builder.append(f.apply(from.getValue()));
				return builder.toString();
			}
		});
	}

	public static <F> String toString(Collection<F> collection, int maxLen) {
		return toString(collection, maxLen, identityFunction);
	}

	public static <F, T extends F> String toString(Collection<T> collection,
			int maxLen, Function<F, String> f) {
		if (collection == null)
			return null;
		StringBuilder builder = new StringBuilder();
		builder.append("{size=");
		builder.append(collection.size());
		builder.append(", [");
		int i = 0;
		for (Iterator<T> iterator = collection.iterator(); iterator.hasNext()
				&& i < maxLen; i++) {
			if (i > 0)
				builder.append(", ");
			builder.append(f.apply(iterator.next()));
		}
		builder.append("]}");
		return builder.toString();
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

	public static Map<Collection<Integer>, double[]> getCombinedMeasures(
			Map<Collection<Integer>, double[]> dimMeasureMap,
			Map<Collection<Integer>, double[]> otherDimMeasureMap,
			Integer measureIndex, Integer otherMeasureIndex) {
		Map<Collection<Integer>, double[]> combinedDimMeasureMap = new HashMap<Collection<Integer>, double[]>();
		for (Map.Entry<Collection<Integer>, double[]> entry : dimMeasureMap
				.entrySet()) {
			Collection<Integer> key = entry.getKey();
			double[] measureVals = entry.getValue();
			double[] combinedMeasures = new double[2];

			combinedMeasures[0] = measureVals[measureIndex];
			double[] otherMeasuresVals = otherDimMeasureMap.get(key);
			if (otherMeasuresVals != null) {
				combinedMeasures[1] = otherMeasuresVals[otherMeasureIndex];
			}
			combinedDimMeasureMap.put(key, combinedMeasures);
		}
		return combinedDimMeasureMap;
	}

	public static <T> Map<Collection<Integer>, ConcurrentHashMap<Long, T>> getTimeseriesData(
			Table table, ICube trimmerEntity) {
		Map<ICube, AggregatedData> aggrData = TransformationEngine.getInstance().transform(
				table, Arrays.<ICube> asList(trimmerEntity), null);
		return (Map<Collection<Integer>, ConcurrentHashMap<Long, T>>) 
				aggrData.get(trimmerEntity).getTimeseries();
	}

	public static <T> Map<Collection<Integer>, T> getAggrData(Table table,
			ICube trimmerEntity) {
		Map<ICube, AggregatedData> aggrData = TransformationEngine.getInstance().transform(
				table, Arrays.<ICube> asList(trimmerEntity), null);
		return (Map<Collection<Integer>, T>) aggrData.get(trimmerEntity).getAggregate();
	}

	public static Table transformAggrTableDimensionsOnly(Table table,
			ICube entity, boolean isAggregate) {
		// transform data
		Map<ICube, AggregatedData> transformedData = TransformationEngine
		.getInstance()
		.transform(table, Utility.asLinkedHashSet(entity), null);

		if (!isAggregate) {
			if (Utility.isDoubleDataType(entity.getOutputMeasures())) {
				ConcurrentHashMap<Collection<Integer>, Map<Long, double[]>> concurrentHashMap = (ConcurrentHashMap<Collection<Integer>, Map<Long, double[]>>) transformedData
						.get(entity).getTimeseries();
				TimeseriesTableImpl retVal = new TimeseriesTableImpl(
						entity.getOutputDimensions(),
						entity.getOutputMeasures(),
						table.getDataTimeGranularity(),
						concurrentHashMap.size(), table.getStartTime(),
						table.getEndTime());
				for (Entry<Collection<Integer>, Map<Long, double[]>> e : concurrentHashMap
						.entrySet())
					retVal.addTimeSeries(FastInts.toArray(e.getKey()),
							e.getValue());
				return retVal;
			} else {
				ConcurrentHashMap<Collection<Integer>, Map<Long, ByteBuffer[]>> concurrentHashMap = (ConcurrentHashMap<Collection<Integer>, Map<Long, ByteBuffer[]>>) transformedData
						.get(entity).getTimeseries();
				TimeseriesTableImpl retVal = new TimeseriesTableImpl(
						entity.getOutputDimensions(),
						entity.getOutputMeasures(),
						table.getDataTimeGranularity(),
						concurrentHashMap.size(), table.getStartTime(),
						table.getEndTime());
				for (Entry<Collection<Integer>, Map<Long, ByteBuffer[]>> e : concurrentHashMap
						.entrySet())
					retVal.addTimeSeriesBitArray(FastInts.toArray(e.getKey()),
							e.getValue());
				return retVal;
			}
		} else {
			if (Utility.isDoubleDataType(entity.getOutputMeasures())) {
				ConcurrentHashMap<Collection<Integer>, double[]> concurrentHashMap = (ConcurrentHashMap<Collection<Integer>, double[]>) transformedData
						.get(entity).getAggregate();
				AggrTableImpl retVal = new AggrTableImpl(
						entity.getOutputDimensions(),
						entity.getOutputMeasures(),
						table.getDataTimeGranularity(),
						concurrentHashMap.size(), table.getStartTime(),
						table.getEndTime());

				for (Entry<Collection<Integer>, double[]> e : concurrentHashMap
						.entrySet())
					retVal.addAggregate(FastInts.toArray(e.getKey()),
							e.getValue());
				return retVal;
			} else {
				ConcurrentHashMap<Collection<Integer>, ByteBuffer[]> concurrentHashMap = (ConcurrentHashMap<Collection<Integer>, ByteBuffer[]>) transformedData
						.get(entity).getAggregate();
				AggrTableImpl retVal = new AggrTableImpl(
						entity.getOutputDimensions(),
						entity.getOutputMeasures(),
						table.getDataTimeGranularity(),
						concurrentHashMap.size(), table.getStartTime(),
						table.getEndTime());

				for (Entry<Collection<Integer>, ByteBuffer[]> e : concurrentHashMap
						.entrySet())
					retVal.addAggregate(FastInts.toArray(e.getKey()),
							e.getValue());
				return retVal;
			}
		}
	}

	public static <T> T[] getTruncatedResult(long maxCount, T[] result) {
		if (maxCount < 0 || maxCount >= result.length) {
			return result;
		}
		return Arrays.copyOfRange(result, 0, (int) maxCount);
	}

	public static <T> T[] getResult(T[] responses, long length, long offset) {

		if (responses == null || responses.length <= offset) {
			return Arrays.copyOfRange(responses, 0, 0);
		}

		if (responses.length <= (offset + length)) {
			return Arrays
					.copyOfRange(responses, (int) offset, responses.length);
		}

		if (length == -1) {
			return Arrays.copyOfRange(responses, (int) offset, responses.length);
		}

		return Arrays.copyOfRange(responses, (int) offset,
				(int) (offset + length));

	}

	@SuppressWarnings("unchecked")
	public static <T> T[] nullArray(T[] responses) {
		T[] result = (T[]) java.lang.reflect.Array.newInstance(responses
				.getClass().getComponentType(), 0);
		return result;
	}

	public static <K> Map<K, Integer> getIndexMap(Collection<K> collection) {
		Map<K, Integer> indexMap = new HashMap<K, Integer>();
		int index = 0;
		for (K measure : collection) {
			indexMap.put(measure, index);
			index++;
		}
		return indexMap;
	}

	public static <K> Map<K, Integer> getIndexMap(Set<K> set,
			Collection<K> collectionReference) {
		Map<K, Integer> indexMap = new HashMap<K, Integer>();
		int index = 0;
		for (K k : collectionReference) {
			if (set.contains(k)) {
				indexMap.put(k, index);
			}
			index++;
		}
		Preconditions.checkState(indexMap.size() == set.size(),
				"collectionReference %s should contain set %s",
				collectionReference, set);
		return indexMap;
	}

	public static <K> int[] getIndices(Collection<K> collection,
			Collection<K> collectionReference) {
		Map<K, Integer> indexMap = getIndexMap(Sets.newHashSet(collection),
				collectionReference);
		int[] result = new int[collection.size()];
		int index = 0;
		for (K k : collection) {
			result[index++] = indexMap.get(k);
		}
		return result;
	}

	public static <K> int getIndex(K key, Collection<K> collectionReference) {
		int index = 0;
		for (K k : collectionReference) {
			if (key.equals(k)) {
				return index;
			}
			index++;
		}
		throw new IllegalArgumentException(key + " not found in "
				+ collectionReference);
	}

	public static String toString(int[][] array2d) {
		StringBuilder sb = new StringBuilder("[");
		boolean first = true;
		for (int[] array1d : array2d) {
			if (first) {
				first = false;
				sb.append(Arrays.toString(array1d));
			} else {
				sb.append(", ");
				sb.append(Arrays.toString(array1d));
			}
		}
		sb.append("]");
		return sb.toString();
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

	// DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL);

	public static String humanReadableTimeStamp(long timestampInSeconds) {
		DateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss z");
		dateFormat.setTimeZone(utcTimeZone);
		return dateFormat.format(new Date(timestampInSeconds * 1000));
	}

	public static Method deriveMethod(Class<?> T, String fieldName) {
		if (null == T)
			return null;
		String methodName = getterMethodName(fieldName);
		Method[] methods = T.getMethods();
		for (Method method : methods) {
			if (method.getName().equalsIgnoreCase(methodName)) {
				return method;
			}
		}
		return deriveMethod(T.getSuperclass(), fieldName);
	}

	private static String getterMethodName(String field) {
		return "get" + field.substring(0, 1).toUpperCase() + field.substring(1);
	}

	public static Iterable<Map<IDimension, Integer>> addToAllFilters(
			final List<Map<IDimension, Integer>> filters,
			final IDimension dimension, final List<Integer> values) {
		return Iterables.concat(Iterables.transform(values,
				new Function<Integer, Iterable<Map<IDimension, Integer>>>() {

			@Override
			public Iterable<Map<IDimension, Integer>> apply(
					Integer value) {
				return addToAllFilters(filters, dimension, value);
			}
		}));
	}

	public static int getTimeExcludingWeekends(long startTime, long endTime,
			TimeGranularity dataTimeGranularity) {
		return (int) (getTimestampCountExcludingWeekends(startTime, endTime,
				dataTimeGranularity) * dataTimeGranularity.getGranularity());
	}

	public static int getTimestampCountExcludingWeekends(long startTime,
			long endTime, TimeGranularity dataTimeGranularity) {
		int[] timeStamps = getHourlyTimestampCountExcludingWeekends(startTime,
				endTime, dataTimeGranularity);
		int total = 0;
		for (int ts : timeStamps) {
			total += ts;
		}
		return total;
	}

	// public static long getCurrentTimestampInSeconds(){
	// return System.currentTimeMillis()/1000;
	// }

	/**
	 * calculates number of bin intervals on an hourly basis excluding weekends
	 * 
	 * @param startTime
	 * @param endTime
	 * @param dataTimeGranularity
	 * @return
	 */
	public static int[] getHourlyTimestampCountExcludingWeekends(
			long startTime, long endTime, TimeGranularity dataTimeGranularity) {

		int[] hourlyIntervals = new int[HOUR_MAX];

		Calendar startCal = Utility.newCalendar();
		startCal.setTimeInMillis(startTime * MILLIS_IN_SECOND);

		Calendar endCal = Utility.newCalendar();
		endCal.setTimeInMillis(endTime * MILLIS_IN_SECOND);

		int startDay = startCal.get(Calendar.DAY_OF_WEEK);
		int endDay = endCal.get(Calendar.DAY_OF_WEEK);

		/*
		 * if start time and end time are for the same day, then return
		 * immediately if it is weekend, else compute bin intervals for this day
		 */
		if (startCal.get(Calendar.YEAR) == endCal.get(Calendar.YEAR)
				&& startCal.get(Calendar.MONTH) == endCal.get(Calendar.MONTH)
				&& startCal.get(Calendar.DATE) == endCal.get(Calendar.DATE)) {

			if (startDay == Calendar.SATURDAY || startDay == Calendar.SUNDAY)
				return hourlyIntervals;
			else
				return getTimestampCount(startCal, endCal, dataTimeGranularity);
		}

		int[] startDayHourlyIntervals = new int[HOUR_MAX]; // ensure this is not
		// null
		int[] endDayHourlyIntervals = new int[HOUR_MAX]; // ensure this is not
		// null

		/*
		 * compute bin intervals between start time and end of the day if it is
		 * not a weekend
		 */
		if (startDay != Calendar.SATURDAY && startDay != Calendar.SUNDAY) {
			Calendar startCalEnd = getDayAfterGivenTime(startCal, 1);
			startDayHourlyIntervals = getTimestampCount(startCal, startCalEnd,
					dataTimeGranularity);
		}

		/*
		 * compute bin intervals between start of the day and end time if it is
		 * not a weekend
		 */
		if (endDay != Calendar.SATURDAY && endDay != Calendar.SUNDAY) {
			Calendar endCalStart = getDayAfterGivenTime(endCal, 0);
			endDayHourlyIntervals = getTimestampCount(endCalStart, endCal,
					dataTimeGranularity);
		}

		/*
		 * compute number of weekdays from the end of day of start time to the
		 * start of day of end time
		 */
		startCal = getDayAfterGivenTime(startCal, 1);
		endCal = getDayAfterGivenTime(endCal, 0);

		int numCompleteWeekdays = getWeekdayCount(startCal, endCal);

		int intervalsInHour = SECS_IN_HOUR
				/ (int) dataTimeGranularity.getGranularity();

		int intervals = numCompleteWeekdays * intervalsInHour;

		for (int i = 0; i < HOUR_MAX; i++)
			hourlyIntervals[i] = startDayHourlyIntervals[i]
					+ endDayHourlyIntervals[i] + intervals;

		return hourlyIntervals;
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

	/**
	 * <code>entityIdDimension</code>
	 * 
	 * @param filters
	 * @param dimension
	 * @param value
	 * @return
	 */
	public static List<Map<IDimension, Integer>> addToAllFilters(
			List<Map<IDimension, Integer>> filters, final IDimension dimension,
			final int value) {

		if (filters == null || filters.size() == 0) {
			filters = new ArrayList<Map<IDimension, Integer>>();
			filters.add(new HashMap<IDimension, Integer>());
		}

		return Lists
				.transform(
						filters,
						new Function<Map<IDimension, Integer>, Map<IDimension, Integer>>() {

							@Override
							public Map<IDimension, Integer> apply(
									Map<IDimension, Integer> filtermap) {
								Map<IDimension, Integer> retVal = new HashMap<IDimension, Integer>(
										filtermap);
								retVal.put(dimension, value);
								return retVal;
							}
						});
	}

	public static Collection<Map<IDimension, Integer>> addToAllFilters(
			Collection<Map<IDimension, Integer>> filters,
			final Map<IDimension, Integer> filter) {
		if ((filters == null || filters.size() == 0)
				&& (filter != null && filters.size() > 0)) {
			filters = new ArrayList<Map<IDimension, Integer>>();
			filters.add(new HashMap<IDimension, Integer>());
		}

		return Collections2
				.transform(
						filters,
						new Function<Map<IDimension, Integer>, Map<IDimension, Integer>>() {

							@Override
							public Map<IDimension, Integer> apply(
									Map<IDimension, Integer> filtermap) {
								Map<IDimension, Integer> retVal = new HashMap<IDimension, Integer>(
										filtermap);
								retVal.putAll(filter);
								return retVal;
							}
						});
	}

	public static Collection<Map<IDimension, Integer>> removeFromAllFilters(
			Collection<Map<IDimension, Integer>> filters,
			final Map<IDimension, Integer> filter) {
		Collection<Map<IDimension, Integer>> retvals = new ArrayList<Map<IDimension, Integer>>();
		Set<Map.Entry<IDimension, Integer>> keyMapEntrySet = filter.entrySet();
		for (Map<IDimension, Integer> filtermap : filters) {
			Map<IDimension, Integer> tempMap = null;
			if (filtermap.entrySet().containsAll(keyMapEntrySet)) {
				tempMap = new HashMap<IDimension, Integer>();
				tempMap.putAll(filtermap);
				Set<Map.Entry<IDimension, Integer>> tempMapEntrySet = tempMap
						.entrySet();
				tempMapEntrySet.removeAll(keyMapEntrySet);
			}
			if (tempMap != null && !tempMap.isEmpty())
				retvals.add(tempMap);
		}
		return retvals;

	}

	public static <V> long size(Iterable<V> collection,
			Function<V, Long> function) {
		long total = 0;
		for (V v : collection) {
			total += function.apply(v);
		}
		return total;
	}

	public static void printMemoryStats() {
		logger.info("FreeMemory before calling GC: {}", Runtime.getRuntime()
				.freeMemory());
		System.gc();
		System.gc();
		logger.info("FreeMemory after calling GC:  {}", Runtime.getRuntime()
				.freeMemory());
	}

	public static <K, V> Map<K, V> toMap(Collection<K> keys, V value) {
		Map<K, V> map = Maps.newHashMap();
		for (K key : keys) {
			map.put(key, value);
		}
		return map;
	}
	
    public static <K, V> Map<K, Map<K, V>> toMap(Map<K, Set<K>> keys, V value) {
        Map<K, Map<K, V>> map = new HashMap<K, Map<K,V>>((int) (keys.size()/.75 + 1));
        for(Entry<K, Set<K>> entry : keys.entrySet()) {
            Map<K, V> innerMap = new HashMap<K, V>((int) (entry.getValue().size()/.75 + 1));
            map.put(entry.getKey(), innerMap);
            for(K key : entry.getValue()) {
                innerMap.put(key, value);
            }
        }
        return map;
    }

    public static <K, V> Map<K, Set<K>> toMapRevert(Map<K, Map<K, V>> map) {
        Map<K, Set<K>> keys = new HashMap<K, Set<K>>();
        for (Entry<K, Map<K, V>> entry : map.entrySet()) {
            keys.put(entry.getKey(), entry.getValue().keySet());
        }
        return keys;
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

	public static Set<LinkedHashMap<IDimension, Integer>> getKeyMap(
			FilterRequest filterRequest, ICube cube) {
		Collection<IDimension> keys = cube.getKeys();
		if (isNullOrEmpty(keys))
			return null;
		Set<LinkedHashMap<IDimension, Integer>> keyMapList = new HashSet<LinkedHashMap<IDimension, Integer>>();
		for (Map<IDimension, Integer> filterMap : filterRequest.getLegacyFilterMap()) {
			LinkedHashMap<IDimension, Integer> keyMap = new LinkedHashMap<IDimension, Integer>();
			for (IDimension key : keys) {
				keyMap.put(key, filterMap.get(key));
			}
			keyMapList.add(keyMap);
		}
		return keyMapList;
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
	
	/*
	 * This method uses timezone specified in RubixProperties
	 */
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

	public static IndexedTable mergeTables(IRequest request,
			Map<IRequest, IndexedTable> dependentTables,
			IMeasure outputMeasure, IMeasure dependentMeasure,
			Set<IMeasure> allMeasures) {
		IndexedTable iTable = null;
		if (request.getRequestDataType() == RequestDataType.Aggregate) {
			iTable = mergeAggregate(request, dependentTables, outputMeasure,
					dependentMeasure, allMeasures);
		} else {
			iTable = mergeTimeSeries(request, dependentTables, outputMeasure,
					dependentMeasure, allMeasures);
		}
		iTable.generateDimensionalKey();
		return iTable;
	}

	private static <T> T compareValues(T obj1, T obj2) {
		if (obj1 == null) {
			obj1 = obj2;
		} else if (!obj1.equals(obj2)) {
			throw new IllegalStateException(String.format("obj1 %s and obj2 %s are not equivalent", obj1.toString(), obj2.toString()));
		}
		return obj1;
	}

	private static IndexedTable mergeTimeSeries(IRequest request,
			Map<IRequest, IndexedTable> dependentTables,
			IMeasure outputMeasure, IMeasure dependentMeasure,
			Set<IMeasure> allMeasures) {
		Map<Collection<Integer>, Map<Long, double[]>> dimToTsToMeasureMap = new HashMap<Collection<Integer>, Map<Long, double[]>>();

		Collection<IDimension> finalDimensions = null;
		TimeGranularity timeGran = null;
		for (IndexedTable currTable : dependentTables.values()) {
			// iterate over all the indexed tables for the given measure
			IRecord currIter = currTable.iterator(0,
					currTable.getNumberOfRecords());
			/*
			 * map of dimension to timeStamp to their index map for finding
			 * index of measure for the matched record.
			 */
			List<IDimension> currDimensions = new ArrayList<IDimension>(
					currTable.getDimensions());
			finalDimensions = compareValues(finalDimensions, currDimensions);
			timeGran = compareValues(timeGran,
					currTable.getDataTimeGranularity());

			while (currIter.hasNext()) {
				/* iterate over each record in the table */
				IRecord currRecord = currIter.next();
				Collection<Integer> dKey = currRecord.getDimensionValues();

				Map<Long, double[]> recordValues = dimToTsToMeasureMap
						.get(dKey);
				if (recordValues == null) {
					recordValues = new HashMap<Long, double[]>();
					dimToTsToMeasureMap.put(dKey, recordValues);
				}
				// iterate over all the timestamps & set measure values in
				// response table
				int srcSTIndex = 0;
				for (long ts : currRecord.getSampleTimes()) {
					if (recordValues.containsKey(ts)) {
						throw new IllegalStateException(
								"multiple measure values for timestamps");
					}
					double currentMeasure = currRecord.getMeasureValue(
							dependentMeasure, srcSTIndex);
					recordValues.put(ts, new double[] { currentMeasure });
					srcSTIndex++;
				}
			}
		}

		TimeseriesTableImpl indexedTable = new TimeseriesTableImpl(
				finalDimensions, allMeasures, timeGran,
				dimToTsToMeasureMap.size(), request.getStartTime(),
				request.getEndTime());

		for (Entry<Collection<Integer>, Map<Long, double[]>> entry : dimToTsToMeasureMap
				.entrySet()) {
			Map<Long, double[]> tsTomeasure = entry.getValue();

			indexedTable
			.addTimeSeries(FastInts.toArray(entry.getKey()),
					tsTomeasure, new int[] { indexedTable
				.getIndexForMeasure(outputMeasure) });

		}
		return new IndexedTable(indexedTable);
	}

	private static IndexedTable mergeAggregate(IRequest request,
			Map<IRequest, IndexedTable> dependentTables,
			IMeasure outputMeasure, IMeasure dependentMeasure,
			Set<IMeasure> allMeasures) {

		Map<Collection<Integer>, Double> dimToTsToMeasureMap = new HashMap<Collection<Integer>, Double>();

		Collection<IDimension> finalDimensions = null;
		TimeGranularity timeGran = null;
		for (IndexedTable currTable : dependentTables.values()) {
			IRecord currIter = currTable.iterator(0,
					currTable.getNumberOfRecords());

			List<IDimension> currDimensions = new ArrayList<IDimension>(
					currTable.getDimensions());
			finalDimensions = compareValues(finalDimensions, currDimensions);
			timeGran = compareValues(timeGran,
					currTable.getDataTimeGranularity());

			while (currIter.hasNext()) {
				IRecord currRecord = currIter.next();
				Collection<Integer> dKey = currRecord.getDimensionValues();
				Double recordValues = dimToTsToMeasureMap.get(dKey);

				double currentMeasure = currRecord.getMeasureValue(
						dependentMeasure, 0);
				if (recordValues != null) { // record exists in response table
					// update value
					dimToTsToMeasureMap
					.put(dKey, recordValues + currentMeasure);

				} else {
					dimToTsToMeasureMap.put(dKey, currentMeasure);
				}
			}
		}
		AggrTableImpl indexedTable = new AggrTableImpl(finalDimensions,
				allMeasures, timeGran, dimToTsToMeasureMap.size(),
				request.getStartTime(), request.getEndTime());
		for (Entry<Collection<Integer>, Double> entry : dimToTsToMeasureMap
				.entrySet()) {
			indexedTable
			.addAggregate(FastInts.toArray(entry.getKey()),
					new double[] { entry.getValue() },
					new int[] { indexedTable
				.getIndexForMeasure(outputMeasure) });
		}
		return new IndexedTable(indexedTable);

	}

	public static LinkedHashSet<IMeasure> deriveDependentMeasures(
			List<IMeasure> measures, IMeasureProcessorMap processorMap) {

		LinkedHashSet<IMeasure> retMeasures = new LinkedHashSet<IMeasure>();

		if (measures == null) {
			return retMeasures;
		}

		for (IMeasure measure : measures) {
			retMeasures.addAll(deriveDependentMeasures(measure, processorMap));
		}

		return retMeasures;

	}

	public static LinkedHashSet<IMeasure> deriveDependentMeasures(
			IMeasure measure, IMeasureProcessorMap processorMap) {
		if (measure.getType() == BASIC) {
			return Utility.<IMeasure> asLinkedHashSet(measure);
		} else {
			LinkedHashSet<IMeasure> measures = new LinkedHashSet<IMeasure>();

			IMeasureProcessor processor = processorMap.getProcessor(measure);

			if (null == processor) {
				return measures;
			}

			List<IMeasure> dependentMeasures = processor.getDependentMeasures();

			for (IMeasure each : dependentMeasures) {
				measures.addAll(deriveDependentMeasures(each, processorMap));
			}
			measures.addAll(dependentMeasures);
			return measures;
		}
	}

    public static boolean isAVSMeasure(Collection<IMeasure> measures) {
        if (!isNullOrEmpty(measures)) {
            return measures.iterator().next().isAttributeValueSketch();
        }
        return false;
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
		//Experimentally calculated overhead for string
		double size = 56;
		
		//Allocates bytes for string characters in multiple of 4
		//(experimentally seen)
		if(length%4 != 0) {
			length = ((length / 4) + 1) * 4;
		}
		
		//Each character takes 2 bytes
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
	
	

	/**
	 * it allocated full backed array without checking its limit. but copies
	 * till its limit
	 * 
	 * @param buf
	 * @return
	 */
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

	public static List<Integer> getSelectedValues(int[] indices,
			Collection<Integer> collection) {
		int[] dimensionValues = FastInts.toArray(collection);
		int[] trimmedDimensionValues = new int[indices.length];
		int i = 0;
		for (int dimensionIndex : indices) {
			trimmedDimensionValues[i++] = dimensionValues[dimensionIndex];
		}
		List<Integer> trimmedDimensionsValueList = FastInts
				.asList(trimmedDimensionValues);
		return trimmedDimensionsValueList;
	}

	public static Integer getSelectedValue(int index,
			Collection<Integer> collection) {
		int[] dimensionValues = FastInts.toArray(collection);
		return dimensionValues[index];
	}

	// Use following to not create unnecessary Object[] when using
	// Lists.newArryaList(Arrays.fill)
	public static <V> List<V> newInitializedArrayList(int initialCapacity) {
		List<V> newArrayList = new ArrayList<V>(initialCapacity);
		for (int i = 0; i < initialCapacity; i++) {
			newArrayList.add(null);
		}
		return newArrayList;
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

	/**
	 * Returns a filtered set of dimensions which are non cached
	 * 
	 * @param dims
	 * @return
	 */
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
	
	public static Set<IGenericDimension> getGenericDimensions(
			Set<IDimension> dimensions) {
		final Map<IDimension, IGenericDimension> dimensionToGenericDimensionMap = ConfigFactory
				.getInstance().getBean(IGenericConfig.class)
				.getDimensionToGenericDimensionMap();
		Set<IGenericDimension> genericDimensions = asSet(dimensions,
				new Function<IDimension, IGenericDimension>() {
			@Override
			public IGenericDimension apply(IDimension input) {
				return dimensionToGenericDimensionMap.get(input);
			}

		});

		return genericDimensions;

	}

	public static boolean isRequestValidForGrowth(Request req,
			long[] previousMonthStartEndTime, String binClass) {
		boolean isValid = false;
		ArrayList<IRequest> reqList = null;
		if (previousMonthStartEndTime[0] >= Controller.getInstance().getFirstBinPersistedTime(
				binClass, BinSource.getDefault().name())) {
			isValid = true;
		}
		return isValid;
	}

	public static boolean isRequestValidForGrowth(Request req,
			long[] previousMonthStartEndTime) {
		boolean isValid = false;
		ArrayList<IRequest> reqList = null;
		if (previousMonthStartEndTime[0] >= Controller.getInstance().getFirstBinPersistedTime(
				req.getTimeGranularity() != null ? req.getTimeGranularity().getName() 
						: ConfigFactory.getInstance() 
						.getBean(TimeGranularity.class).getName(), BinSource.getDefault().name())) {
			isValid = true;
		}
		return isValid;
	}

	public static IRequest getRequestForGrowth(Request req,
			long[] previousMonthStartEndTime) {
		Flow flow = ConfigFactory.getInstance().getBean(Flow.class);
		Collection<ICube> cubeCollection = flow.getCube(req);
		if (cubeCollection != null && cubeCollection.size() > 0) {
			ICube bCube = cubeCollection.iterator().next();
			if (bCube.getBinClass() != null) {
				if (Utility.isRequestValidForGrowth(req,
						previousMonthStartEndTime, bCube.getBinClass())) {
					return req;
				}
			} else {
				if (Utility.isRequestValidForGrowth(req,
						previousMonthStartEndTime)) {
					return req;
				}
			}
		} else {
			if (Utility.isRequestValidForGrowth(req, previousMonthStartEndTime)) {
				return req;
			}
		}
		return null;
	}

	public static IQueryRequest getRequestForGrowth(IQueryRequest queryRequest,
			long[] previousMonthStartEndTime) {
		Request reqFlow = DataServiceUtil.getRequestConvertFunction(
				RequestDataType.Aggregate, new HashSet<IGenericDimension>())
				.apply(queryRequest);

		SubQueryFilter subQueryFilter = DataServiceUtil
				.convertToFlowReadableFilterMap(queryRequest.getFilterRequest());
		reqFlow.setFilterRequest(subQueryFilter.getFilterRequest());
		IRequest requestGrowth = getRequestForGrowth(reqFlow,
				previousMonthStartEndTime);
		if (requestGrowth != null)
			return queryRequest;
		return null;

	}

	public static final String defaultTimeZoneStr = "0:0";

	public static int calendarField = Calendar.HOUR_OF_DAY;

	public static String getTimeZone(Object timeZone) {
		return timeZone != null ? timeZone.toString() : RubixProperties.TimeZone.getValue();
	}

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

	public static Criterion buildSearchCriteria(List<List<SearchCriterion>> subqueryCriteria) {

		Criterion subQueryCriterion = null;
		for (List<SearchCriterion> orList:subqueryCriteria) {
			Criterion lastCriterion = null;
			for (SearchCriterion searchCriterion:orList) {

				Criterion orCriterion = Operator.valueOf(searchCriterion.getOperator())
						.getCriteriaConverter().apply(searchCriterion);
				if (lastCriterion == null)
					lastCriterion = orCriterion;
				else
					lastCriterion = Restrictions.and(lastCriterion, orCriterion);
			}
			if (subQueryCriterion == null)
				subQueryCriterion = lastCriterion;
			else 
				subQueryCriterion = Restrictions.or(lastCriterion, subQueryCriterion);
			//			criteria.add(Operator.valueOf(searchCriterion.getOperator())
			//					.getCriteriaConverter().apply(searchCriterion));
		}
		return subQueryCriterion;

	}
	
	public static boolean useAggregate(Collection<IMeasure> measures, boolean isAggregate, ICacheType type){
		//if range cache is enabled, then use the isAggregate property
		if(type == CacheType.ALL_HOUR_BY_DAY) {
			return false;
		}
		
		if(RubixProperties.BinReplay.getBooleanValue()){
			/*
			 * in case of bin replay don't use aggregate cache
			 * as point rubix cache bin replay is not supported
			 */
			return false;
		}
	
		if(!measures.isEmpty() && !measures.iterator().next().isSubtractible())
			return false;
		
		return isAggregate;
	}
	
	public static <N,M> Map<Integer, M> mergeMulti(boolean isDoubleDataType, List<Map<Integer, N>> tupleMaps, int[] measureIndices,
			List<IMeasure> measures, Long granularity, Map<Integer, ArrayList<Integer>> tupleIds, DataMergeType mergeType, Integer segmentId, RubixCache<?, ?> cache){
		if(isDoubleDataType)
			return operationMulti(onDoubleMulti, tupleMaps, measureIndices, tupleIds, measures, granularity,mergeType, segmentId, cache);
		else
			return operationMulti(onByteBufferMulti, tupleMaps, measureIndices, tupleIds, measures, granularity,mergeType, segmentId, cache);
	}
	
	private interface Operation<N,M> {
    	Map<Integer, M> apply(Map<Integer, M> resultTupleMeasureMap,
        		Map<Integer, N> sourceTupleMeasureMap, Collection<IMeasure> measureNames);
    }
	
	private interface OperationMulti<N,M> {
    	Map<Integer, M> apply(List<Map<Integer, N>> tupleMeasureMaps, int[] measureIndices, Map<Integer, ArrayList<Integer>> tupleIds, 
    			List<IMeasure> requestedMeasures, Long granularity, DataMergeType mergeType, Integer segmentId, RubixCache<?, ?> cache);
    }
	
	public static Double mergeDoubles(INumericFunction function, 
			Collection<Double> values, Collection<Integer> dimensionValues, Collection<IDimension> dimensions) {
		Double result = 0.0;
		for (Double val : values) {
			result = function.compute(result, val, new AggregationContext(dimensionValues, dimensions, DataMergeType.HORIZONTAL));
		}
		return result;
	}
    
    static OperationMulti<ByteBuffer,double[]> onDoubleMulti = new OperationMulti<ByteBuffer, double[]>() {
        public Map<Integer, double[]> apply(List<Map<Integer, ByteBuffer>> tupleMeasureMaps, int[] measureIndices, Map<Integer, ArrayList<Integer>> tupleIds
        		, List<IMeasure> requestedMeasures, Long granularity, DataMergeType mergeType, Integer segmentId, RubixCache<?, ?> cache) {
        	Map<Integer, double[]> resultTupleMap = new HashMap<Integer, double[]>(getMaxSize(tupleMeasureMaps),1f);
        	
        	for(int i=0; i<tupleMeasureMaps.size(); i++){
        		Map<Integer, ByteBuffer> tupleMap = tupleMeasureMaps.get(i);
        		for (Entry<Integer, ByteBuffer> tupleMapEntry : tupleMap.entrySet()) {
        			Integer tupleID = tupleMapEntry.getKey();
					if (resultTupleMap.containsKey(tupleID)
							|| (tupleIds != null && !tupleIds.keySet().contains(tupleID)))
        				continue;
        			
        			List<ByteBuffer> allTupleMeasures = new ArrayList<ByteBuffer>(tupleMeasureMaps.size() - i);
        			allTupleMeasures.add(tupleMapEntry.getValue());
        			
        			//get the same tuple from all the maps which are being merged
        			for(int j = i+1; j < tupleMeasureMaps.size(); j++){
        				Map<Integer, ByteBuffer> currentTupleMap = tupleMeasureMaps.get(j);
        				ByteBuffer measures = currentTupleMap.get(tupleID);
        				if (measures != null) {
        					allTupleMeasures.add(measures);
                		}
        			}
        			
        			//merge all measures for the current tuple
    				double[] resultMeasures = null;
    				for(ByteBuffer measures: allTupleMeasures){
    					if(resultMeasures == null){
//    						resultMeasures = Arrays.copyOf(measures, measures.length);
    						resultMeasures = new double[requestedMeasures.size()];
    						for (int measureCounter = 0 ; measureCounter < measureIndices.length ; measureCounter++) {
    							resultMeasures[measureCounter] = measures.getDouble(measureIndices[measureCounter]*8);
    						}
    					} else{
	    					Iterator<IMeasure> itr = requestedMeasures.iterator();
	    					for (int measureCounter = 0 ; measureCounter < measureIndices.length ; measureCounter++) {
	    	        			IMeasure measureName = itr.next();
	    	        			resultMeasures[measureCounter] = 
	    	        					measureName.getAggregationFunction().compute(
	    	        							resultMeasures[measureCounter], measures.getDouble(measureIndices[measureCounter]*8)
	    	        							, new AggregationContext(cache.getDimensionValuesForTupleId(tupleID, segmentId), cache.getDimensions(), mergeType));
	    	        		}
    					}
    				}
        			resultTupleMap.put(tupleID, resultMeasures);
        		}
        	}
        	
        	return resultTupleMap;
        }
        
        @SuppressWarnings("rawtypes")
    	private int getMaxSize(List<Map<Integer, ByteBuffer>> tupleMaps){
    		
    		int max = 0;
    		for(Map tupleMap: tupleMaps)
    			if(tupleMap.size() > max)
    				max = tupleMap.size();
    		
    		return max;
    	}

        public String toString() {
            return "onDoubleMulti";
        }
    };
    
    static OperationMulti<ByteBuffer[],ByteBuffer[]> onByteBufferMulti = new OperationMulti<ByteBuffer[],ByteBuffer[]>() {
        public Map<Integer, ByteBuffer[]> apply(List<Map<Integer, ByteBuffer[]>> tupleMeasureMaps, int[] measureIndices, Map<Integer, ArrayList<Integer>> tupleIds
        		, List<IMeasure> requestedMeasures, Long granularity, DataMergeType mergeType, Integer segmentId, RubixCache<?, ?> cache) {
        	
        	Set<Integer> allTupleIds = new HashSet<Integer>();
        	for (Map<Integer, ByteBuffer[]> tupleMeasureMap : tupleMeasureMaps) {
        		if (allTupleIds == null) {
        			allTupleIds = Sets.newHashSet(tupleMeasureMap.size());
        		}
        		allTupleIds.addAll(tupleMeasureMap.keySet());
        	}
        	Map<Integer, ByteBuffer[]> resultTupleMap = new HashMap<Integer, ByteBuffer[]>(allTupleIds.size(),1f);
        	for(int i=0; i<tupleMeasureMaps.size(); i++){
        		Map<Integer, ByteBuffer[]> tupleMap = tupleMeasureMaps.get(i);
        		for (Integer tupleID : tupleMap.keySet()) {
					if (resultTupleMap.containsKey(tupleID)
							|| (tupleIds != null && !tupleIds.containsKey(tupleID)))
        				continue;
        			
					int counter;
					byte[] emptyBuffersPosition = new byte[tupleMeasureMaps.size()];
        			for (counter = 0; counter < i ; counter++) {
        				emptyBuffersPosition[counter] = 1;
        			}
        			emptyBuffersPosition[counter] = 0;
					List<ByteBuffer[]> allTupleMeasures = new ArrayList<ByteBuffer[]>(tupleMeasureMaps.size() - i);
        			allTupleMeasures.add(tupleMap.get(tupleID));
        			
        			//get the same tuple from all the maps which are being merged
        			for(int j = i+1; j < tupleMeasureMaps.size(); j++){
        				Map<Integer, ByteBuffer[]> currentTupleMap = tupleMeasureMaps.get(j);
        				ByteBuffer[] measures = currentTupleMap.get(tupleID);
        				if (measures != null) {
        					allTupleMeasures.add(measures);
        					emptyBuffersPosition[j] = 0;
                		} else {
                			emptyBuffersPosition[j] = 1;
                		}
        			}
        			resultTupleMap.put(tupleID, computeAll(requestedMeasures, allTupleMeasures,
        					measureIndices, mergeType,
        					emptyBuffersPosition, granularity,
        					tupleIds != null ? tupleIds.get(tupleID) : null));
        		}
        	}
        	
        	
        	return resultTupleMap;
        }
        
        @SuppressWarnings("rawtypes")
    	private int getMaxSize(List<Map<Integer, ByteBuffer[]>> tupleMaps){
    		
    		int max = 0;
    		for(Map tupleMap: tupleMaps)
    			if(tupleMap.size() > max)
    				max = tupleMap.size();
    		
    		return max;
    	}

        public String toString() {
            return "onByteBufferMulti";
        }
    };
  
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

    private static <N,M> Map<Integer, M> operation(Operation operation,
    		Map<Integer, M> resultTupleMeasureMap,
    		Map<Integer, N> sourceTupleMeasureMap, Collection<IMeasure> measureNames) {
    	return operation.apply(resultTupleMeasureMap, sourceTupleMeasureMap, measureNames);
    }
    
    private static <N,M> Map<Integer, M> operationMulti(OperationMulti operation,
    		List<Map<Integer, N>> tupleMaps, int[] measureIndices, Map<Integer, ArrayList<Integer>> tupleIds,
    		List<IMeasure> measures, Long granularity, DataMergeType mergeType, Integer segmentId, RubixCache<?, ?> cache) {
    	return operation.apply(tupleMaps, measureIndices, tupleIds, measures, granularity,mergeType, segmentId, cache);
    	
    }
    
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
