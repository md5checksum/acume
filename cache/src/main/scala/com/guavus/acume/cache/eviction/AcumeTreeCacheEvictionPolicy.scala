package com.guavus.acume.cache.eviction

import com.guavus.acume.cache.utility.Utility
import java.util.Calendar
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import scala.collection.immutable.TreeMap
import com.guavus.acume.cache.common.LevelTimestamp
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.common.Cube
import scala.collection.mutable.MutableList

/**
 * @author archit.thakur
 *
 */

object AcumeTreeCacheEvictionPolicy {

  def getRangeStartTime(lastBinTimeStamp: Long, level: Long, numPoints: Int): Long = {
    val rangeEndTime = Utility.floorFromGranularity(lastBinTimeStamp, level)
    val rangeStartTime = 
    if (level == TimeGranularity.MONTH.getGranularity) {
      val cal = Utility.newCalendar()
      cal.setTimeInMillis(rangeEndTime * 1000)
      cal.add(Calendar.MONTH, -1 * numPoints)
      cal.getTimeInMillis / 1000
    } else if (level == TimeGranularity.DAY.getGranularity) {
      val cal = Utility.newCalendar()
      cal.setTimeInMillis(rangeEndTime * 1000)
      cal.add(Calendar.DAY_OF_MONTH, -1 * numPoints)
      cal.getTimeInMillis / 1000
    } else if (level == TimeGranularity.WEEK.getGranularity) {
      val cal = Utility.newCalendar()
      cal.setTimeInMillis(rangeEndTime * 1000)
      cal.add(Calendar.DAY_OF_MONTH, -1 * numPoints * 7)
      cal.getTimeInMillis / 1000
    } else if ((level == TimeGranularity.THREE_HOUR.getGranularity) || 
      (level == TimeGranularity.FOUR_HOUR.getGranularity)) {
      val cal = Utility.newCalendar()
      cal.setTimeInMillis(rangeEndTime * 1000)
      val endOffset = cal.getTimeZone.getOffset(cal.getTimeInMillis) / 1000
      val tempRangeStartTime = rangeEndTime - numPoints * level
      cal.setTimeInMillis(tempRangeStartTime * 1000)
      val startOffset = cal.getTimeZone.getOffset(cal.getTimeInMillis) / 1000
      tempRangeStartTime + (endOffset - startOffset)
    } else {
      rangeEndTime - numPoints * level
    }
    rangeStartTime
  }
}

class AcumeTreeCacheEvictionPolicy(cube: Cube, conf: AcumeCacheConf) extends EvictionPolicy(cube, conf) {

  def getEvictableCandidate(list: List[LevelTimestamp]): Option[LevelTimestamp] = {
    
    val variableretentionmap = cube.levelPolicyMap
    var count = 0
    var _$evictableCandidate: Option[LevelTimestamp] = None
    for(leveltimestamp <- list) yield {
      
      if(isEvictiable(leveltimestamp, variableretentionmap)) {
        if(count == 0)
          _$evictableCandidate = Some(leveltimestamp)
        count += 1
      }
    }
    if(count > 1)
      println("WARNING: More than one evictable candidate found.")
    _$evictableCandidate
  }
  
  private def getPriority(timeStamp: Long, level: Long, variableRetentionMap: Map[Long, Int]): Int = {
    if (!variableRetentionMap.contains(level)) return 0
    val numPoints = variableRetentionMap.get(level).getOrElse(throw new RuntimeException("Level not in VariableRetentionMap."))
    val lastBinTime = conf.get(ConfConstants.lastbinpersistedtime).toLong //Controller.getInstance.getLastBinPersistedTime(ConfigFactory.getInstance.getBean(classOf[TimeGranularity])
        //.getName, BinSource.getDefault.name(), Controller.RETRY_COUNT)
      if (timeStamp > AcumeTreeCacheEvictionPolicy.getRangeStartTime(lastBinTime, level, numPoints)) 1 else 0
  }

  private def isEvictiable(levelTimestamp: LevelTimestamp, variableRetentionMap: Map[Long, Int]): Boolean = {
    if (getPriority(levelTimestamp.timestamp, levelTimestamp.level.localId, variableRetentionMap) == 0) true else false
  }

  private def intializeMetaData(variableRetentionMap: Map[Long, Int]): HashMap[Long, Long] = {
    val lastBinTime = conf.get(ConfConstants.lastbinpersistedtime).toLong //Controller.getInstance.getLastBinPersistedTime(ConfigFactory.getInstance.getBean(classOf[TimeGranularity])
      //.getName, BinSource.getDefault.name(), Controller.RETRY_COUNT)
    val map = HashMap[Long, Long]()
    for ((key, value) <- variableRetentionMap) {
      val numPoints = value
      map.put(key, AcumeTreeCacheEvictionPolicy.getRangeStartTime(lastBinTime, key, numPoints))
    }
    map
  }
}