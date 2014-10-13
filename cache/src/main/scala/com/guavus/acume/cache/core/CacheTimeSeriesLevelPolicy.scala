package com.guavus.acume.cache.core

import scala.collection.immutable.SortedMap
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.utility.Utility
import scala.collection.mutable.MutableList
import java.util.Calendar

class CacheTimeSeriesLevelPolicy(levelMap: SortedMap[Long, Int]) extends CacheTimeSeriesPolicyTrait {

  override def getAggregationIntervals(): HashMap[MutableList[Long], Long] = {
    val aggregationIntervals = HashMap[MutableList[Long], Long]()
    val endTime = 0//Controller.getInstance.getLastBinPersistedTime(binclass, binSource)
    val startTime = 0//Controller.getInstance.getFirstBinPersistedTime(binclass, binSource)
    for ((key, value) <- levelMap){
      val roundedEndTime = Utility.floorFromGranularity(endTime, key)
      val roundedStartTime: Long = 
        if (key == TimeGranularity.MONTH.getGranularity) {
          val cal = Utility.newCalendar()
          cal.setTimeInMillis(roundedEndTime * 1000)
          cal.add(Calendar.MONTH, -1 * value)
          cal.getTimeInMillis / 1000
        } else {
          roundedEndTime - key * value
        }
      val intervals = MutableList[Long]()
      if (roundedStartTime > 0) {
        intervals.+=(roundedStartTime)
        intervals.+=(roundedEndTime)
        aggregationIntervals.put(intervals, key)
      } else {
        intervals.+=(startTime)
        intervals.+=(roundedEndTime)
        aggregationIntervals.put(intervals, key)
      }
    }
    aggregationIntervals
  }

  override def getLevelToUse(startTime: Long, endTime: Long): Long = {
    val lastBinTime = 0//Controller.getInstance.getLastBinPersistedTime(binclass, binSource, Controller.RETRY_COUNT)
    for ((key, value) <- levelMap) {
      val level = key
      val points = value
      val rangeEndTime = Utility.floorFromGranularity(lastBinTime, level)
      val rangeStartTime = 
        if (level == TimeGranularity.MONTH.getGranularity) {
          val cal = Utility.newCalendar
          cal.setTimeInMillis(rangeEndTime * 1000)
          cal.add(Calendar.MONTH, -1 * points)
          cal.getTimeInMillis / 1000
        } else {
          rangeEndTime - points * level
        }
      if (startTime >= rangeStartTime) return level
    }
    levelMap.lastKey
  }

  override def copy(): CacheTimeSeriesPolicyTrait = {
    new CacheTimeSeriesLevelPolicy(levelMap)
  }

  override def getAllLevels(): List[Long] = levelMap.keySet.toList

}