package com.guavus.acume.core

import java.util.Calendar
import scala.collection.JavaConversions._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.scheduler.Controller
import com.guavus.rubix.query.remote.flex.TimeZoneInfo
import com.guavus.rubix.query.remote.flex.ZoneInfoParams
import com.guavus.rubix.cache.Interval
import com.guavus.acume.core.scheduler.ICacheAvalabilityUpdatePolicy
import scala.collection.mutable.HashMap

object PSUserService {

  private var logger: Logger = LoggerFactory.getLogger(classOf[PSUserService])
}

class PSUserService {

  val controller = ConfigFactory.getInstance.getBean(classOf[Controller])
  val defaultBinSource = AcumeContextTraitUtil.acumeConf.get(ConfConstants.acumecorebinsource)
  def getTimeRange(): Array[Long] = Array[Long](controller.getFirstBinPersistedTime(defaultBinSource), controller.getLastBinPersistedTime(defaultBinSource))
  
  def getZoneInfo(ids : java.util.List[String] , params : ZoneInfoParams) : java.util.List[TimeZoneInfo] = {
		val timeGran = ConfigFactory.getInstance().getBean(classOf[TimeGranularity]).getGranularity()
		val timeGranularity = TimeGranularity.getTimeGranularity(timeGran)
		val timeRange = getTimeRange()
		val cal = Calendar.getInstance(Utility.getTimeZone(AcumeContextTraitUtil.acumeConf.get(ConfConstants.timezone)))
		var startYear = 0
		if(params != null && params.getStartYear() != null){
			startYear = Integer.parseInt(params.getStartYear());
		}
		else{
			cal.setTimeInMillis(timeRange(0) * 1000)
			startYear = cal.get(Calendar.YEAR) - 1
		}
		var endYear = 0
		if(params != null && params.getEndYear() != null){
			endYear = Integer.parseInt(params.getEndYear())
		}
		else{
			cal.setTimeInMillis(timeRange(1) * 1000)
			endYear = cal.get(Calendar.YEAR) + 1
		}
		try {
			return Utility.getTimeZoneInfo(ids.toList, startYear, endYear, AcumeContextTraitUtil.acumeConf.get(ConfConstants.timezonedbPath))
		} catch {
		  case e :Exception => 
			throw new RuntimeException(e.getMessage())
		} 
	}
  
  def getInstaTimeInterval(binSource : String) : Map[Long, (Long, Long)] = {
    if(binSource == null){
      controller.getInstaTimeIntervalForBinSource(defaultBinSource)
    }
    else{
    controller.getInstaTimeIntervalForBinSource(binSource)
    }
  }

  def getAcumeAvailability : java.util.Map[String, java.util.Map[Long, Interval]] = {
    val map: HashMap[String, HashMap[Long, Interval]] = ICacheAvalabilityUpdatePolicy.getICacheAvalabiltyUpdatePolicy.getCacheAvalabilityMap
    mapAsJavaMap(map.map(x => (x._1, mapAsJavaMap(x._2))))
  }

/*
Original Java:
package com.guavus.acume.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

|**
 * @author Akhil Swain
 * 
 *|
public class PSUserService {

	private static Logger logger = LoggerFactory.getLogger(PSUserService.class);

	public long[] getTimeRange() {
		return new long[]{1416441600L, 1416474000L};
	}
	
	public List<TimeZoneInfo> getZoneInfo(List<String>ids , ZoneInfoParams params){
		long timeGran = ConfigFactory.getInstance().getBean(TimeGranularity.class).getGranularity();
		TimeGranularity timeGranularity = TimeGranularity.getTimeGranularity(timeGran);
		long firstBinTime = Controller.getInstance()
				.getFirstBinPersistedTime(timeGranularity.getName(),BinSource.getDefault().name());
		long lastBinTime = Controller.getInstance()
				.getLastBinPersistedTime(timeGranularity.getName(),BinSource.getDefault().name());
		Calendar cal = Calendar.getInstance(Utility.getTimeZone(RubixProperties.TimeZone.getValue()));
		int startYear;
		if(params != null && params.getStartYear() != null){
			startYear = Integer.parseInt(params.getStartYear());
		}
		else{
			cal.setTimeInMillis(firstBinTime * 1000);
			startYear = cal.get(Calendar.YEAR) - 1;
		}
		int endYear;
		if(params != null && params.getEndYear() != null){
			endYear = Integer.parseInt(params.getEndYear());
		}
		else{
			cal.setTimeInMillis(lastBinTime * 1000);
			endYear = cal.get(Calendar.YEAR) + 1;
		}
		try {
			return Utility.getTimeZoneInfo(ids, startYear, endYear);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		} 
	}
//	
//	public Map<String,Map<List<Long>,Long>> getAllAggregationIntervals(String binClass) {
//		Map<String,Map<List<Long>,Long>> aggregationIntervals = new HashMap<String, Map<List<Long>,Long>>();
//		Map<String, Interval> allBinSources = getAllBinSources(binClass);
//		
//		for (Map.Entry<String, Interval> entry : allBinSources.entrySet()) {
//			aggregationIntervals.put(entry.getKey(),getAllAggregationIntervalsForBinSource(binClass, entry.getKey()));
//		}
//		return aggregationIntervals;
//	}
//	
//	public Map<List<Long>,Long> getAllAggregationIntervalsForBinSource(String binClass, String binSource) {
//		ICacheTimeSeriesPolicy bean = ConfigFactory.getInstance().getBean(ICacheTimeSeriesPolicy.class);
//		return bean.getAggregationIntervals(binSource, binClass);
//	}
//
//	public long[] getTimeRange(Long timeGran) {
//		TimeGranularity timeGranularity = null;
//		if(timeGran == null)
//		{
//			timeGran = ConfigFactory.getInstance().getBean(TimeGranularity.class).getGranularity();
//		}
//		if (timeGran != null) {
//			timeGranularity = TimeGranularity.getTimeGranularity(timeGran);
//			if(timeGranularity == null || timeGranularity.getName() == null || timeGranularity.getName().isEmpty()){
//				logger.error("No bin class exists for the time granularity: {}", timeGran);
//				throw new RuntimeException("No bin class exists for the time granularity: " + timeGran);
//			}
//		}
//
//		long[] binTimes = null;
//		try {
//			long firstBinTime = Controller.getInstance()
//					.getFirstBinPersistedTime(timeGranularity.getName(),BinSource.getDefault().name());
//			long lastBinTime = Controller.getInstance()
//					.getLastBinPersistedTime(timeGranularity.getName(),BinSource.getDefault().name());
//
//			if (firstBinTime > lastBinTime)
//				throw new IllegalStateException(
//						String.format(
//								"firstBinPersistedTime: %d is greater than lastBinPersistedTime: %d",
//								new Object[] { firstBinTime, lastBinTime }));
//
//
//			binTimes = new long[] { firstBinTime, lastBinTime };
//		} catch (Throwable t) {
//			logger.error("error getting bin persisted time:", t);
//			throw new RuntimeException(t);
//		}
//		return binTimes;
//	}
//    @Deprecated
//	public List<BinTimeRange> getTimeRanges() {
//		try {
//			String[] classes = ConfigFactory.getInstance().getSEClasses();
//			List<BinTimeRange> timeRanges = new LinkedList<BinTimeRange>();
//
//			for (String aClass : classes) {
//				TimeGranularity timeGranularity = TimeGranularity.getTimeGranularityByName(aClass);
//				if (timeGranularity != null) {
//					long firstBinTime = Controller.getInstance().getFirstBinPersistedTime(timeGranularity.getName(),BinSource.getDefault().name());
//					long lastBinTime = Controller.getInstance().getLastBinPersistedTime(timeGranularity.getName(),BinSource.getDefault().name());
//					timeRanges.add(new BinTimeRange(timeGranularity.getGranularity(), firstBinTime, lastBinTime-timeGranularity.getGranularity()));
//				}
//			}
//
//			return timeRanges;
//		} catch (Throwable t) {
//			logger.error("error getting bin persisted time:", t);
//			throw new RuntimeException(t);
//		}
//	}
//
//	public long[] getTimeRangeBinClass(String binClass) {
//		if(binClass == null) {
//			Long tGran = null;
//			return getTimeRange(tGran);
//		}
//		long[] binTimes = null;
//		try{
//			long firstBinTime = Controller.getInstance()
//					.getFirstBinPersistedTime(binClass,BinSource.getDefault().name());
//			long lastBinTime = Controller.getInstance()
//					.getLastBinPersistedTime(binClass,BinSource.getDefault().name());
//
//			if (firstBinTime > lastBinTime)
//				throw new IllegalStateException(
//						String.format(
//								"firstBinPersistedTime: %d is greater than lastBinPersistedTime: %d",
//								new Object[] { firstBinTime, lastBinTime }));
//
//
//			binTimes = new long[] { firstBinTime, lastBinTime };
//		}catch(Throwable t){
//			logger.error("error getting bin persisted time:", t);
//			throw new RuntimeException(t);
//		}
//		return binTimes;
//	}
//	
//	public long[][] getTimeRanges(String[] binClasses){
//        long[][] timeRanges = new long[binClasses.length][];
//
//        int i = 0;
//        for(String binClass: binClasses){
//        	timeRanges[i++] = getTimeRangeBinClass(binClass);
//        }
//
//        return timeRanges;
//	}
//	
//	public Map<String,Interval> getAllBinSources(String binClass)
//	{
//		Map<String,Map<Long,Interval>>  binSourceToAggrTimeMap = Controller.getInstance().getAllBinSources(binClass);
//		Map<String,Interval> binSourceToIntervalMap = new HashMap<String,Interval>();
//		for(Entry<String, Map<Long, Interval>> entry: binSourceToAggrTimeMap.entrySet()){
//			String binSource = entry.getKey();
//			binSourceToIntervalMap.put(binSource, entry.getValue().get(Controller.DEFAULT_AGGR_INTERVAL));
//		}
//		return binSourceToIntervalMap;
//	}
//	
//	public Long[] getRubixTimeRange() {
//			Long timeGran = ConfigFactory.getInstance().getBean(TimeGranularity.class).getGranularity();
//			TimeGranularity timeGranularity = null;
//			if (timeGran != null) {
//				timeGranularity = TimeGranularity.getTimeGranularity(timeGran);
//				if(timeGranularity == null || timeGranularity.getName() == null || timeGranularity.getName().isEmpty()){
//					logger.error("No bin class exists for the time granularity: {}", timeGran);
//					throw new RuntimeException("No bin class exists for the time granularity: " + timeGran);
//				}
//			}
//			return getRubixTimeRange(timeGranularity.getName());
//	}
//	
//	
//	public Long[] getRubixTimeRange(String binClass) {
//		
//		Map<String, Map<Long, Interval>> binSourceToAvailabilityMap = QueryRequestPrefetchTaskManager.getInstance().getBinclassToBinSourceToRubixAvailability().get(binClass);
//		if(binSourceToAvailabilityMap == null) {
//			return new Long[] { 0L, 0L };
//		}
//		Map<Long, Interval> granToAvailabilityMap = binSourceToAvailabilityMap.get(BinSource.getDefault().name());
//		if(granToAvailabilityMap == null) {
//			return new Long[] { 0L, 0L }; 
//		}
//		
//		Long timeGran = ConfigFactory.getInstance().getBean(TimeGranularity.class).getGranularity();
//		TimeGranularity timeGranularity = null;
//		if (timeGran != null) {
//			timeGranularity = TimeGranularity.getTimeGranularity(timeGran);
//			if(timeGranularity == null || timeGranularity.getName() == null || timeGranularity.getName().isEmpty()){
//				logger.error("No bin class exists for the time granularity: {}", timeGran);
//				throw new RuntimeException("No bin class exists for the time granularity: " + timeGran);
//			}
//		}
//		
//		Interval interval = granToAvailabilityMap.get(timeGran);
//		if(interval == null) {
//			return new Long[] { 0L, 0L };
//		}
//		long endTime = getLastBinTime(interval.getEndTime(),binClass,BinSource.getDefault().name(),timeGran);
//		if(endTime > interval.getStartTime()){
//			return new Long[] { interval.getStartTime(), endTime };
//		}else{
//			return new Long[] { 0L, 0L };
//		}
//	}
//	
//	private long getLastBinTime(long endTime, String binClass, String binSource,Long timeGran) {
//		return Utility.floorFromGranularity(Math.min(endTime,Controller.getInstance().getLastBinPersistedTime(binClass, binSource)),timeGran);
//	}
//
//	public Map<String, Map<Long, Interval>> getRubixTimeIntervalForBinClass(String binClass) {
//		
//		Map<String, Map<Long, Interval>> binSourceToAvailabilityMap = QueryRequestPrefetchTaskManager.getInstance().getBinclassToBinSourceToRubixAvailability().get(binClass);
//		if(binSourceToAvailabilityMap == null) {
//			return new HashMap<String, Map<Long,Interval>>(); 
//		}
//		Map<String, Map<Long, Interval>> binSourceToAvaMap = new HashMap<String, Map<Long,Interval>>();
//		for(String binSource: binSourceToAvailabilityMap.keySet()){
//			Map<Long, Interval> granToInterval = new HashMap<Long, Interval>();
//			
//			for(Long timeGran:binSourceToAvailabilityMap.get(binSource).keySet()){
//				Interval avaInterval = binSourceToAvailabilityMap.get(binSource).get(timeGran);
//				long endTime = getLastBinTime(avaInterval.getEndTime(), binClass, binSource, timeGran);
//				if(endTime > avaInterval.getStartTime()){
//					Interval interval = new Interval(avaInterval.getStartTime(),endTime);
//					granToInterval.put(timeGran, interval);
//				}
//			}
//			binSourceToAvaMap.put(binSource, granToInterval);
//		}
//		return binSourceToAvaMap;
//	}
//	
//	public String getRgeUrl() {
//		return RubixProperties.RgeUrl.getValue();
//	}
//	
//	|**
//	 * @author r.ganesh
//	 * @return Rubix data availability
//	 *|
//	public Map<String, Map<String, Map<String, Interval>>> getRubixDataAvailability() {
//		Map<String, Map<String, Map<Long, Interval>>> binClassToBinSourceToAvailabilityMap = QueryRequestPrefetchTaskManager
//				.getInstance().getBinclassToBinSourceToRubixAvailability();
//		if (binClassToBinSourceToAvailabilityMap == null) {
//			return new HashMap<String, Map<String, Map<String, Interval>>>();
//		}
//		Map<String, Map<String, Map<String, Interval>>> binClassToBinSourceToAvaMap = new HashMap<String, Map<String, Map<String, Interval>>>();
//		for (Entry<String, Map<String, Map<Long, Interval>>> binClassEntry : binClassToBinSourceToAvailabilityMap
//				.entrySet()) {
//			String binClass = binClassEntry.getKey();
//			Map<String, Map<String, Interval>> binSourceToAvaMap = new HashMap<String, Map<String, Interval>>();
//			for (Entry<String, Map<Long, Interval>> binSourceEntry : binClassEntry.getValue().entrySet()) {
//				String binSource = binSourceEntry.getKey();
//				Map<String, Interval> granToInterval = new HashMap<String, Interval>();
//				for (Entry<Long, Interval> timeGranEntry : binSourceEntry.getValue().entrySet()) {
//					Long timeGran = timeGranEntry.getKey();
//					Interval avaInterval = timeGranEntry.getValue();
//					long endTime = getLastBinTime(avaInterval.getEndTime(), binClass, binSource, timeGran);
//					if (endTime > avaInterval.getStartTime()) {
//						Interval interval = new Interval(avaInterval.getStartTime(), endTime);
//						granToInterval.put(timeGran.toString(), interval);
//					}
//				}
//				binSourceToAvaMap.put(binSource, granToInterval);
//			}
//			binClassToBinSourceToAvaMap.put(binClass, binSourceToAvaMap);
//		}
//		return binClassToBinSourceToAvaMap;
//	}
//	
//	|**
//	 * @author r.ganesh
//	 * @return Insta data availability
//	 *|
//	public Map<String, Map<String, Map<String, Interval>>> getInstaDataAvailability() {
//		Map<String, Map<String, Map<Long, Interval>>> binClassToBinSourceToAvailabilityMap = Controller.getInstance()
//				.getBinClassToBinSources();
//		if (binClassToBinSourceToAvailabilityMap == null) {
//			return new HashMap<String, Map<String, Map<String, Interval>>>();
//		}
//		Map<String, Map<String, Map<String, Interval>>> binClassToBinSourceToAvaMap = new HashMap<String, Map<String, Map<String, Interval>>>();
//		for (Entry<String, Map<String, Map<Long, Interval>>> binClassEntry : binClassToBinSourceToAvailabilityMap
//				.entrySet()) {
//			String binClass = binClassEntry.getKey();
//			Map<String, Map<String, Interval>> binSourceToAvaMap = new HashMap<String, Map<String, Interval>>();
//			for (Entry<String, Map<Long, Interval>> binSourceEntry : binClassEntry.getValue().entrySet()) {
//				String binSource = binSourceEntry.getKey();
//				Map<String, Interval> granToInterval = new HashMap<String, Interval>();
//				for (Entry<Long, Interval> timeGranEntry : binSourceEntry.getValue().entrySet()) {
//					Long timeGran = timeGranEntry.getKey();
//					Interval avaInterval = timeGranEntry.getValue();
//					granToInterval.put(timeGran.toString(), avaInterval);
//				}
//				binSourceToAvaMap.put(binSource, granToInterval);
//			}
//			binClassToBinSourceToAvaMap.put(binClass, binSourceToAvaMap);
//		}
//		return binClassToBinSourceToAvaMap;
//	}
}
*/
}