package com.guavus.acume.core.scheduler

import scala.collection.JavaConversions._
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.core.Interval
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeConf
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.guavus.acume.core.AcumeContextTrait
import java.lang.IllegalArgumentException
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.DataService
import com.guavus.acume.core.AcumeContextTraitUtil

class VariableGranularitySchedulerPolicy extends ISchedulerPolicy {

  val schedulerVariableRetentionMap: Map[Long, Int] = Utility.getLevelPointMap(AcumeContextTraitUtil.acumeConf.getSchedulerVariableRetentionMap).map(x=> x._1.level -> x._2)

  val cachePopulationMap: HashMap[PrefetchCubeConfiguration, HashMap[String, HashMap[Long, Long]]] = new HashMap[PrefetchCubeConfiguration, HashMap[String, HashMap[Long, Long]]]()

  override def getIntervalsAndLastUpdateTime(startTime: Long, endTime: Long, cubeConfiguration: PrefetchCubeConfiguration, isFirstTimeRun: Boolean, optionalParams: HashMap[String, Any], taskManager: QueryRequestPrefetchTaskManager): PrefetchLastCacheUpdateTimeAndInterval = {
    val prefetchLastCacheUpdateTimeAndInterval = new PrefetchLastCacheUpdateTimeAndInterval()
    cachePopulationMap.get(cubeConfiguration) match {
      case None => cachePopulationMap.+=(cubeConfiguration -> HashMap[String, HashMap[Long, Long]]())
      case _ => 
    }
    val binsource = optionalParams.get(QueryPrefetchTaskProducer.BIN_SOURCE).get.asInstanceOf[String]
    cachePopulationMap.get(cubeConfiguration).get.get(binsource) match {
      case None => cachePopulationMap.get(cubeConfiguration).get.+=(binsource -> HashMap[Long, Long]())
      case _ => 
    }
    val binSourceToIntervalMap = cachePopulationMap.get(cubeConfiguration).get.get(binsource).get
    val lastBinTime = optionalParams.get(QueryPrefetchTaskProducer.LAST_BIN_TIME).get.asInstanceOf[Interval]
    val instance = Utility.newCalendar()
    val version = optionalParams.get(QueryPrefetchTaskProducer.VERSION).get.asInstanceOf[Int]
    
    if (version != taskManager.getVersion) {
      throw new IllegalStateException("View changed current version " + version + " and new version is " + taskManager.getVersion)
    }
    for ((level,noOfRequests) <- schedulerVariableRetentionMap) {
      var tempStartTime = binSourceToIntervalMap.get(level).getOrElse(startTime)
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
//        prefetchLastCacheUpdateTimeAndInterval.getIntervals
//        createIntervalAndLastUpdateTime(level, tempStartTime, endTime, lastBinTime.getStartTime, schedulerVariableRetentionMap.get(level).get, binSourceToIntervalMap)
//        createIntervalAndLastUpdateTime(level, tempStartTime, endTime, lastBinTime.getStartTime, schedulerVariableRetentionMap.get(level).get, binSourceToIntervalMap)
//        cubeConfiguration.getTopCube.getTimeGranularityValue
//        mergeTimeIntervals(createIntervalAndLastUpdateTime(level, tempStartTime, endTime, lastBinTime.getStartTime, schedulerVariableRetentionMap.get(level).get, binSourceToIntervalMap), level, cubeConfiguration.getTopCube.getTimeGranularityValue)
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
    val maxDuration = cubeGranularity * AcumeContextTraitUtil.acumeConf.getInstaComboPoints
    val noOfIntervalsToBeCombined = (if (maxDuration / level > AcumeContextTraitUtil.acumeConf.getSchedulerVariableRetentionCombinePoints) AcumeContextTraitUtil.acumeConf.getSchedulerVariableRetentionCombinePoints else maxDuration / level).toInt
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
    cachePopulationMap.clear
  }
}