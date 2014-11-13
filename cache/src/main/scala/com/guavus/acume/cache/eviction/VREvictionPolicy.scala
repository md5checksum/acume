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

object VREvictionPolicy {

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

//  def main(args: Array[String]) {
//    val variableRetentionMap = Utility.getLevelPointMap("5m:26000;1h:100;1M:12").asInstanceOf[TreeMap]
//    val flashVariableRetentionMap = Utility.getLevelPointMap("5m:0").asInstanceOf[TreeMap]
//    val policy = new TreeRubixCacheVariableRetentionPolicy(variableRetentionMap, flashVariableRetentionMap)
//    val primaryCacheList = new ArrayList[Any]()
//    var time = 1356998400
//    for (i <- 0 until 26000) {
//      primaryCacheList.add(new LevelTimestamp(300L, time))
//      if (time == Utility.floorFromGranularity(time, 2592000L)) {
//        primaryCacheList.add(new LevelTimestamp(2592000L, time))
//      }
//      if (time == Utility.floorFromGranularity(time, 3600L)) {
//        primaryCacheList.add(new LevelTimestamp(3600L, time))
//      }
//      time += 300
//    }
//    println(primaryCacheList.size)
//    for (j <- 0 until 100) {
//      val startTime = System.currentTimeMillis()
//      val map = policy.intializeMetaData()
//      for (key <- primaryCacheList) {
//        policy.isEvictiable(key, map)
//      }
//      println("time taken " + (System.currentTimeMillis() - startTime))
//    }
//    System.exit(1)
//  }
}

class VREvictionPolicy(conf: AcumeCacheConf) extends EvictionPolicy(conf) {

  def getEvictableCandidate(list: List[LevelTimestamp], optional: Any*): LevelTimestamp = {
    
    assert(optional.length == 1, throw new RuntimeException("VREvictionPolicy requires one argument cube."))
    
    val variableretentionmap = optional(0).asInstanceOf[Cube].levelPolicyMap
    for(leveltimestamp <- list) {
      
      if(isEvictiable(leveltimestamp, variableretentionmap))
        return leveltimestamp
    }
    null
  }
  
  private def getPriority(timeStamp: Long, level: Long, variableRetentionMap: Map[Long, Int]): Int = {
    if (!variableRetentionMap.contains(level)) return 0
    val numPoints = variableRetentionMap.get(level).getOrElse(throw new RuntimeException("Level not in VariableRetentionMap."))
    val lastBinTime = conf.get(ConfConstants.lastbinpersistedtime).toLong //Controller.getInstance.getLastBinPersistedTime(ConfigFactory.getInstance.getBean(classOf[TimeGranularity])
        //.getName, BinSource.getDefault.name(), Controller.RETRY_COUNT)
      if (timeStamp > VREvictionPolicy.getRangeStartTime(lastBinTime, level, numPoints)) 1 else 0
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
      map.put(key, VREvictionPolicy.getRangeStartTime(lastBinTime, key, numPoints))
    }
    map
  }
}
