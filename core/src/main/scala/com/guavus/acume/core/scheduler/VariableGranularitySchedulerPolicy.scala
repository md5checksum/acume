package com.guavus.acume.core.scheduler

import VariableGranularitySchedulerPolicy._
import scala.collection.JavaConversions._
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.core.Interval
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeConf
import java.lang.IllegalArgumentException
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

object VariableGranularitySchedulerPolicy {

//  def main(args: Array[String]) {
////    RubixProperties.SchedulerVariableGranularityMap.setValue("1h:720")
//    val startTime = 1361941200
//    val endTime = 1365120000
//    val variableGranularitySchedulerPolicy = new VariableGranularitySchedulerPolicy(new AcumeConf)
//    val binMap = new scala.collection.mutable.HashMap[String, Any]()
//    binMap.put(QueryPrefetchTaskProducer.BIN_SOURCE, "SE")
//    binMap.put(QueryPrefetchTaskProducer.VERSION, 0)
//    binMap.put(QueryPrefetchTaskProducer.LAST_BIN_TIME, new Interval(startTime, endTime))
////    new AcumeConf.setTimeZone.setValue("GMT")
//    var intervals: scala.collection.mutable.HashSet[Interval] = null
//    val cubeConfig = new PrefetchCubeConfiguration()
//    println(intervals)
//    var start = startTime
//    while (start <= endTime) {
//      println(Utility.humanReadableTimeStamp(start))
//      intervals = variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(start, start + 86400, cubeConfig, true, binMap).getIntervals
//      println(intervals)
//      start += 86400
//    }
//    binMap.put(QueryPrefetchTaskProducer.BIN_SOURCE, "SE1")
//    System.exit(0)
//  }
//}
}

class VariableGranularitySchedulerPolicy(acumeConf : AcumeConf) extends AbstractSchedulerPolicy {

  var schedulerVariableRetentionMap: Map[Long, Int] = Utility.getLevelPointMap(acumeConf.getSchedulerVariableRetentionMap)

  var cachePopulationMap: HashMap[PrefetchCubeConfiguration, HashMap[String, HashMap[Long, Long]]] = new HashMap[PrefetchCubeConfiguration, HashMap[String, HashMap[Long, Long]]]()

//  if (acumeConf.getSchedulerVariableRetentionCombinePoints != 1) {
//    RubixProperties.AggregateFromTimeseriesData.setValue("true")
//  }

  override def getIntervalsAndLastUpdateTime(startTime: Long, endTime: Long, cubeConfiguration: PrefetchCubeConfiguration, isFirstTimeRun: Boolean, optionalParams: HashMap[String, Any], taskManager: QueryRequestPrefetchTaskManager): PrefetchLastCacheUpdateTimeAndInterval = {
    val cachePopulationMap = this.cachePopulationMap
    val prefetchLastCacheUpdateTimeAndInterval = new PrefetchLastCacheUpdateTimeAndInterval()
    if (cachePopulationMap.getOrElse(cubeConfiguration, null) == null) {
      cachePopulationMap.+=(cubeConfiguration -> HashMap[String, HashMap[Long, Long]]())
    }
    if (cachePopulationMap.get(cubeConfiguration).get.get(optionalParams.get(QueryPrefetchTaskProducer.BIN_SOURCE).get.asInstanceOf[String]).getOrElse({null}) == null) {
      cachePopulationMap.get(cubeConfiguration).get.+=(optionalParams.get(QueryPrefetchTaskProducer.BIN_SOURCE).get.asInstanceOf[String] -> HashMap[Long, Long]())
    }
    val binSourceToIntervalMap = cachePopulationMap.get(cubeConfiguration).get.get(optionalParams.get(QueryPrefetchTaskProducer.BIN_SOURCE).get.asInstanceOf[String]).get
    val lastBinTimeInterval = optionalParams.get(QueryPrefetchTaskProducer.LAST_BIN_TIME).get.asInstanceOf[Interval]
    val lastBinTime = lastBinTimeInterval
    val instance = Utility.newCalendar()
    val version = optionalParams.get(QueryPrefetchTaskProducer.VERSION).get.asInstanceOf[java.lang.Integer]
    if (version != taskManager.getVersion) {
      throw new IllegalStateException("View changed current version " + version + " and new version is " + taskManager.getVersion)
    }
    for ((level,noOfRequests) <- schedulerVariableRetentionMap) {
      var tempStartTime = binSourceToIntervalMap.get(level).getOrElse({startTime})
      var flag = false
      
      if (isFirstTimeRun) {
        var availableTime = Utility.floorFromGranularity(lastBinTime.getEndTime, level)
        var i = 0
        while (i < noOfRequests) {i += 1
        availableTime = Utility.getPreviousTimeForGranularity(availableTime, level, instance)
        }
        if (availableTime >= endTime) {
          flag = true
        } else if (!flag && availableTime >= startTime) {
          tempStartTime = availableTime
        }
      }
      if (!flag && tempStartTime < endTime) {
        prefetchLastCacheUpdateTimeAndInterval.getIntervals.addAll(mergeTimeIntervals(createIntervalAndLastUpdateTime(level, tempStartTime, endTime, lastBinTime.getStartTime, schedulerVariableRetentionMap.get(level).get, binSourceToIntervalMap), level, cubeConfiguration.getTopCube.getTimeGranularityValue))
      }
    }
    val values = binSourceToIntervalMap.values.toArray[Long]
    java.util.Arrays.sort(values)
    if (values.length == 0) {
      prefetchLastCacheUpdateTimeAndInterval.setCacheLastUpdateTime(endTime)
    } else {
      prefetchLastCacheUpdateTimeAndInterval.setCacheLastUpdateTime(values(0))
    }
    prefetchLastCacheUpdateTimeAndInterval.setCacheEndTimeMap( scala.collection.mutable.HashMap[Long, Long]() ++ binSourceToIntervalMap)
    prefetchLastCacheUpdateTimeAndInterval
  }

  private def mergeTimeIntervals(duration: List[Interval], level: java.lang.Long, cubeGranularity: Long): List[Interval] = {
    val maxDuration = cubeGranularity * acumeConf.getInstaComboPoints
    val noOfIntervalsToBeCombined = (if (maxDuration / level > acumeConf.getSchedulerVariableRetentionCombinePoints) acumeConf.getSchedulerVariableRetentionCombinePoints else maxDuration / level).toInt
    if (noOfIntervalsToBeCombined <= 1) {
      return duration
    }
    val combinedList = new ArrayBuffer[Interval]()
    var i = 0
    while (i < duration.size) {
      if (i + noOfIntervalsToBeCombined <= duration.size) {
        combinedList.add(new Interval(duration.get(i + noOfIntervalsToBeCombined - 1).getStartTime, duration.get(i).getEndTime, duration.get(i).getEndTime, level))
      } else {
        combinedList.add(new Interval(duration.get(duration.size - 1).getStartTime, duration.get(i).getEndTime, duration.get(i).getEndTime, level))
      }
      i = i + noOfIntervalsToBeCombined
    }
    combinedList.toList
  }

  private def createIntervalAndLastUpdateTime(level: java.lang.Long, startTime: Long, endTime: Long, instaStartTime: Long, noOfRequests: Int, binSourceToIntervalMap: scala.collection.mutable.HashMap[Long, Long]): List[Interval] = {
    val floorEndTime = Utility.floorFromGranularity(endTime, level)
    val ceilStartTime = Utility.ceilingFromGranularity(startTime, level)
    var tempEndTime = floorEndTime
    val listOfIntervals = new ArrayBuffer[Interval]()
    val instance = Utility.newCalendar()
    var internalTempStartTime = Utility.getPreviousTimeForGranularity(floorEndTime, level, instance)
    var i = 0
    while (i < noOfRequests && internalTempStartTime >= ceilStartTime) {
      listOfIntervals.add(new Interval(internalTempStartTime, tempEndTime, tempEndTime, level))
      tempEndTime = internalTempStartTime
      internalTempStartTime = Utility.getPreviousTimeForGranularity(internalTempStartTime, level, instance)
      i += 1
    }
    if (floorEndTime > instaStartTime) {
      binSourceToIntervalMap.put(level, floorEndTime)
    } else {
      binSourceToIntervalMap.put(level, instaStartTime)
    }
    listOfIntervals.toList
  }

  override def getCeilOfTime(time: Long): Long = time

  override def clearState() {
    cachePopulationMap = new scala.collection.mutable.HashMap[PrefetchCubeConfiguration, scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[Long, Long]]]()
  }

/*
Original Java:
package com.guavus.rubix.scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.guavus.rubix.cache.Interval;
import com.guavus.rubix.configuration.ConfigFactory;
import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.Controller;
import com.guavus.rubix.util.Utility;

public class VariableGranularitySchedulerPolicy extends AbstractSchedulerPolicy {

	Map<Long,Integer> schedulerVariableRetentionMap;
	Map<PrefetchCubeConfiguration,Map<String,Map<Long,Long>>> cachePopulationMap = new HashMap<PrefetchCubeConfiguration,Map<String ,Map<Long, Long>>>();
	
	

	public VariableGranularitySchedulerPolicy() {
		schedulerVariableRetentionMap = Utility.getLevelPointMap(RubixProperties.SchedulerVariableGranularityMap.getValue());
		if(RubixProperties.SchedulerVariableRetentionCombinePoints.getIntValue() != 1){
			//Setting this property as true when comboPoint is not 1. Without it this will not work
			RubixProperties.AggregateFromTimeseriesData.setValue("true");
		}
	}
	@Override
	public PrefetchLastCacheUpdateTimeAndInterval getIntervalsAndLastUpdateTime(
			long startTime, long endTime,
			PrefetchCubeConfiguration cubeConfiguration,
			boolean isFirstTimeRun, Map<String, Object> optionalParams) {
		
		Map<PrefetchCubeConfiguration, Map<String, Map<Long, Long>>> cachePopulationMap = this.cachePopulationMap ;
		
		PrefetchLastCacheUpdateTimeAndInterval prefetchLastCacheUpdateTimeAndInterval = new PrefetchLastCacheUpdateTimeAndInterval();
		if(cachePopulationMap.get(cubeConfiguration) == null) {
			cachePopulationMap.put(cubeConfiguration, new HashMap<String, Map<Long,Long>>());
		}
		if(cachePopulationMap.get(cubeConfiguration).get(optionalParams.get(QueryPrefetchTaskProducer.BIN_SOURCE)) == null) {
			cachePopulationMap.get(cubeConfiguration).put((String) optionalParams.get(QueryPrefetchTaskProducer.BIN_SOURCE), new HashMap<Long, Long>());
		}
		Map<Long,Long> binSourceToIntervalMap = cachePopulationMap.get(cubeConfiguration).get(optionalParams.get(QueryPrefetchTaskProducer.BIN_SOURCE));
		Map<Long,Interval> lastBinTimeInterval = (Map<Long,Interval>) optionalParams.get(QueryPrefetchTaskProducer.LAST_BIN_TIME);
		Interval lastBinTime = lastBinTimeInterval.get(Controller.DEFAULT_AGGR_INTERVAL);
		Calendar instance = Utility.newCalendar();
		int version = (Integer)optionalParams.get(QueryPrefetchTaskProducer.VERSION);
		QueryRequestPrefetchTaskManager taskManager = ConfigFactory.getInstance().getBean(QueryRequestPrefetchTaskManager.class);
		if(version != taskManager.getVersion()) {
			throw new IllegalStateException("View changed current version " + version + " and new version is " + taskManager.getVersion());
		}
		for (Long level : schedulerVariableRetentionMap.keySet()) {
			int noOfRequests = schedulerVariableRetentionMap.get(level);
			
			long tempStartTime = (binSourceToIntervalMap.get(level) == null) ? startTime
					: binSourceToIntervalMap.get(level);
			if(isFirstTimeRun) {
				long availableTime = Utility.floorFromGranularity(lastBinTime.getEndTime(), level);
				for(int i=0;i<noOfRequests;i++,availableTime =Utility.getPreviousTimeForGranularity(availableTime, level, instance));
				if(availableTime >= endTime) {
					continue;
				} else if(availableTime >= startTime) {
					tempStartTime = availableTime;
				}
			}
			if(tempStartTime < endTime) {
				prefetchLastCacheUpdateTimeAndInterval.getIntervals().addAll(mergeTimeIntervals(createIntervalAndLastUpdateTime(level, 
						tempStartTime, endTime,lastBinTime.getStartTime(),
						schedulerVariableRetentionMap.get(level), binSourceToIntervalMap), level, 
						cubeConfiguration.getTopCube().getTimeGranularity().getGranularity()));
			}
		}
		//return the lowest time which map has
		Long[] values = binSourceToIntervalMap.values().toArray(new Long[0]);
		Arrays.sort(values);
		if(values.length == 0) {
			prefetchLastCacheUpdateTimeAndInterval.setCacheLastUpdateTime(endTime);	
		} else {
			prefetchLastCacheUpdateTimeAndInterval.setCacheLastUpdateTime(values[0]);
		}
		prefetchLastCacheUpdateTimeAndInterval.setCacheEndTimeMap(new HashMap<Long,Long>(binSourceToIntervalMap));
		return prefetchLastCacheUpdateTimeAndInterval;
	}
	
	private List<Interval> mergeTimeIntervals(List<Interval> duration, Long level, long cubeGranularity) {
		long maxDuration = cubeGranularity * RubixProperties.InstaComboPoints.getIntValue();
		int noOfIntervalsToBeCombined = (int) (maxDuration/level > RubixProperties.SchedulerVariableRetentionCombinePoints.getIntValue() ? RubixProperties.SchedulerVariableRetentionCombinePoints.getIntValue():maxDuration/level);
		if(noOfIntervalsToBeCombined <= 1) {
			return duration;
		}
		List<Interval> combinedList =  new ArrayList<Interval>();
		for (int i =0; i< duration.size();i=i+noOfIntervalsToBeCombined) {
			if(i + noOfIntervalsToBeCombined <= duration.size()) {
				combinedList.add(new Interval(duration.get(i + noOfIntervalsToBeCombined - 1).getStartTime(),duration.get(i).getEndTime(),duration.get(i).getEndTime(),level));
			} else {
				combinedList.add(new Interval(duration.get(duration.size() -1).getStartTime(), duration.get(i).getEndTime(),duration.get(i).getEndTime(),level));
			}
		}
		return combinedList;
	}
	
	private List<Interval> createIntervalAndLastUpdateTime(Long level, long startTime, long endTime, long instaStartTime,int noOfRequests, Map<Long,Long> binSourceToIntervalMap) {
		long floorEndTime = Utility.floorFromGranularity(endTime, level);
		long ceilStartTime = Utility.ceilingFromGranularity(startTime, level);
		long tempEndTime = floorEndTime;
		List<Interval> listOfIntervals = new ArrayList<Interval>();
		Calendar instance = Utility.newCalendar();
			long internalTempStartTime = Utility.getPreviousTimeForGranularity(floorEndTime, level, instance);
			for (int i = 0; i < noOfRequests
					&& internalTempStartTime >= ceilStartTime; i++) {
				listOfIntervals.add(new Interval(internalTempStartTime,
						tempEndTime, tempEndTime, level));
				tempEndTime = internalTempStartTime;
				internalTempStartTime = Utility.getPreviousTimeForGranularity(internalTempStartTime, level, instance);
			}
		if(floorEndTime > instaStartTime) {
			 binSourceToIntervalMap.put(level, floorEndTime);
		} else {
			binSourceToIntervalMap.put(level, instaStartTime);
		}
		return listOfIntervals;
	}

	@Override
	public long getCeilOfTime(long time) {
		return time;
	}
	
	@Override
	public void clearState() {
		cachePopulationMap = new HashMap<PrefetchCubeConfiguration, Map<String,Map<Long,Long>>>();
	}
	
	
	public static void main(String[] args) {
		RubixProperties.SchedulerVariableGranularityMap.setValue("1h:720");
		long startTime = 1361941200;
		long endTime = 1365120000;
		VariableGranularitySchedulerPolicy variableGranularitySchedulerPolicy = new VariableGranularitySchedulerPolicy();
		
		Map<String,Object> binMap = new HashMap<String, Object>();
		binMap.put(QueryPrefetchTaskProducer.BIN_SOURCE, "SE");
		binMap.put(QueryPrefetchTaskProducer.VERSION, 0);
		binMap.put(QueryPrefetchTaskProducer.LAST_BIN_TIME, new Interval(startTime, endTime));
		RubixProperties.TimeZone.setValue("GMT");
		Set<Interval> intervals = null;
		PrefetchCubeConfiguration cubeConfig = new PrefetchCubeConfiguration();
//		intervals = variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(endTime-86400, endTime, cubeConfig, true, binMap).getIntervals();
		System.out.println(intervals);
		for(long start = startTime ; start<=endTime; start +=86400 )
		{
			System.out.println(Utility.humanReadableTimeStamp(start));
			intervals = variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(start,start+86400, cubeConfig, true, binMap).getIntervals();
			
			System.out.println(intervals);
		}
		
//		
//		
//		intervals = variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(endTime, endTime+86400, new PrefetchCubeConfiguration(), true, binMap).getIntervals();
//		System.out.println(intervals);
//		
//		intervals = variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(endTime+86400, endTime+5*86400, new PrefetchCubeConfiguration(), true, binMap).getIntervals();
//		System.out.println(intervals);
		
//		 intervals = variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(endTime+5*86400, endTime+20*86400, new PrefetchCubeConfiguration(), true, binMap).getIntervals();
//		System.out.println(intervals);
		
		
		binMap.put(QueryPrefetchTaskProducer.BIN_SOURCE, "SE1");
//		variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(1361769553, 1361776753, new PrefetchCubeConfiguration(), true, null).getIntervals()
//		System.out.println(variableGranularitySchedulerPolicy.getIntervalsAndLastUpdateTime(startTime, endTime, new PrefetchCubeConfiguration(), false, binMap).getIntervals());
		System.exit(0);
	}

}

*/
}