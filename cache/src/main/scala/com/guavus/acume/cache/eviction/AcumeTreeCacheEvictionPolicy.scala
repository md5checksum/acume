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
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.core.Level
import java.util.concurrent.ConcurrentMap
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory

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

class AcumeTreeCacheEvictionPolicy(cube: Cube, cacheContext : AcumeCacheContextTrait) extends EvictionPolicy(cube, cacheContext) {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCacheEvictionPolicy])

  def getMemoryEvictableCandidate(list: Map[LevelTimestamp, AcumeTreeCacheValue]): Option[LevelTimestamp] = {
    getEvictableCandidate(list.filter(_._2.isInMemory), cube.levelPolicyMap)
  }
  
  def getDiskEvictableCandidate(list: Map[LevelTimestamp, AcumeTreeCacheValue]): Option[LevelTimestamp] = {
    getEvictableCandidate(list, cube.diskLevelPolicyMap)
  }
  
  def getEvictableCandidate(list: Map[LevelTimestamp, AcumeTreeCacheValue], variableretentionmap : Map[Level, Int]): Option[LevelTimestamp] = {
    
    var count = 0
    var _$evictableCandidate: Option[LevelTimestamp] = None
    for((leveltimestamp, _x) <- list) yield {
      
      if(isEvictiable(leveltimestamp, variableretentionmap)) {
        if(count == 0)
          _$evictableCandidate = Some(leveltimestamp)
        count += 1
      }
    }
    if(count > 1)
      logger.warn("WARNING: More than one evictable candidate found.")
    //logger.info("Evicting {}", _$evictableCandidate.get.toString())
    _$evictableCandidate
  }
  
  private def getPriority(timeStamp: Long, level: Long, aggregationLevel: Long, variableRetentionMap: Map[Level, Int]): Int = {
    if (!variableRetentionMap.contains(new Level(level))) return 0
    val numPoints = variableRetentionMap.get(new Level(level)).getOrElse(throw new RuntimeException("Level not in VariableRetentionMap."))
    val lastBinTime = cacheContext.getLastBinPersistedTime(cube.binsource) //Controller.getInstance.getLastBinPersistedTime(ConfigFactory.getInstance.getBean(classOf[TimeGranularity])
        //.getName, BinSource.getDefault.name(), Controller.RETRY_COUNT)
    val rangeStarTime = AcumeTreeCacheEvictionPolicy.getRangeStartTime(lastBinTime, level, numPoints)
    var timeStampTobeChecked = timeStamp
    if(aggregationLevel != level) {
      // This is a combined point
      // Check if the last child of this combined point is evictable or not
      timeStampTobeChecked = Utility.getPreviousTimeForGranularity(Utility.getNextTimeFromGranularity(timeStamp, aggregationLevel, Utility.newCalendar()), level, Utility.newCalendar())
      logger.info("Checking combined point ki eviction {} ", timeStampTobeChecked)
    }
    if(timeStampTobeChecked >= rangeStarTime) 1 else 0
  }

  private def isEvictiable(levelTimestamp: LevelTimestamp, variableRetentionMap: Map[Level, Int]): Boolean = {
    if (getPriority(levelTimestamp.timestamp, levelTimestamp.level.localId, levelTimestamp.aggregationLevel.localId, variableRetentionMap) == 0) true else false
  }

  private def intializeMetaData(variableRetentionMap: Map[Long, Int]): HashMap[Long, Long] = {
    val lastBinTime = cacheContext.getLastBinPersistedTime(cube.binsource) //Controller.getInstance.getLastBinPersistedTime(ConfigFactory.getInstance.getBean(classOf[TimeGranularity])
      //.getName, BinSource.getDefault.name(), Controller.RETRY_COUNT)
    val map = HashMap[Long, Long]()
    for ((key, value) <- variableRetentionMap) {
      val numPoints = value
      map.put(key, AcumeTreeCacheEvictionPolicy.getRangeStartTime(lastBinTime, key, numPoints))
    }
    map
  }
}
