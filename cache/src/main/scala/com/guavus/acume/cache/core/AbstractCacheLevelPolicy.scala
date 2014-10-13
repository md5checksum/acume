package com.guavus.acume.cache.core

import scala.collection.mutable.LinkedList
import scala.collection.mutable.Map
import scala.collection.mutable.MutableList
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.acume.cache.utility.Utility

abstract class AbstractCacheLevelPolicy(protected var baseLevel: Long) extends CacheLevelPolicyTrait {

  def getBaseLevel(): Long = baseLevel

  override def getRequiredIntervals(startTime: Long, endTime: Long): Map[Long, MutableList[Long]] = {
    val result = Map[Long, MutableList[Long]]()
    val maxInterval = findMaxInterval(startTime, endTime)
    addIntervals(result, startTime, endTime, maxInterval)
    result
  }

  private def addIntervals(intervals: Map[Long, MutableList[Long]], startTime: Long, endTime: Long, level: Long): Map[Long, MutableList[Long]] = {
    val startTimeCeiling = Utility.ceilingFromGranularity(startTime, level)
    val endTimeFloor = Utility.floorFromGranularity(endTime, level)
    if (endTimeFloor > startTimeCeiling) {
      var currentLevels = intervals.get(level)
      currentLevels match{
        case None => currentLevels = Some(MutableList[Long]())
        intervals.put(level, new LinkedList[Long]())
      }
      intervals.get(level).get.addAll(Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level))
      if (startTimeCeiling > startTime) {
        addIntervals(intervals, startTime, startTimeCeiling, getLowerLevel(level))
      }
      if (endTime > endTimeFloor) {
        addIntervals(intervals, endTimeFloor, endTime, getLowerLevel(level))
      }
    } else {
      addIntervals(intervals, startTime, endTime, getLowerLevel(level))
    }
    intervals
  }

  private def addForwardIntervals(intervals: Map[Long, MutableList[Long]], startTime: Long, endTime: Long, level: Long): Map[Long, MutableList[Long]] = {
    var time = startTime
    var stepSize = level
    var breakCondition = false
    while (time < endTime
        && !breakCondition) {
      if (time + stepSize <= endTime) {
        var times = intervals.get(stepSize)
        times match{
          case None => times = Some(MutableList())
          intervals.put(stepSize, times)
        }
        intervals.get(stepSize).get.add(time)
        time += stepSize
      } else {
        if (stepSize == getBaseLevel) breakCondition = true 	
        stepSize = getChildrenLevel(stepSize)
      }
    }
    intervals
  }

  private def addBackwardIntervals(intervals: Map[Long, MutableList[Long]], startTime: Long, endTime: Long, level: Long): Map[Long, MutableList[Long]] = {
    var time = endTime
    var stepSize = level
    var breakCondition = false
    while (time >= startTime && !breakCondition) {
      if (time - stepSize >= startTime) {
        var times = intervals.get(stepSize)
        times match{
          case None =>
          intervals.put(stepSize, MutableList())
        }
        intervals.get(stepSize).get.add(time - stepSize)
        time -= stepSize
      } else {
        if (stepSize == getBaseLevel) breakCondition = true
        stepSize = getChildrenLevel(stepSize)
      }
    }
    if (time > startTime) {
      var times = intervals.get(getBaseLevel)
      times match{
        case None => intervals.put(getBaseLevel, MutableList().add(time - getBaseLevel))
      }
    }
    intervals
  }

  private def findMaxInterval(startTime: Long, endTime: Long): Long = {
    val duration = endTime - startTime
    var maxInterval = getBaseLevel
    var breakCondition = false
    while (duration > maxInterval && !breakCondition) {
      val maxIntervalTemp = getParentLevel(maxInterval)
      maxInterval =  
        if (maxIntervalTemp != -1) { 
          
          maxIntervalTemp
        } 
        else { 
          
          breakCondition = true
          maxInterval
        }
    }
    maxInterval
  }

  override def getParentInterval(time: Long, level: Long): Long = {
    val parentLevel = getParentLevel(level)
    if (parentLevel == -1) return parentLevel
    Utility.floorFromGranularity(time, parentLevel)
  }

  override def getFloorToLevel(time: Long, level: Long): Long = {
    Utility.floorFromGranularity(time, level)
  }

  override def getCeilingToLevel(time: Long, level: Long): Long = {
    Utility.ceilingFromGranularity(time, level)
  }

  override def getChildrenIntervals(startTime: Long, level: Long): MutableList[Long] = {
    val children = MutableList[Long]()
    val childrenLevel = getChildrenLevel(level)
    if (childrenLevel != -1){
      val endTime = Utility.getNextTimeFromGranularity(startTime, level, Utility.newCalendar())
      children.addAll(Utility.getAllIntervals(startTime, endTime, childrenLevel))
    }
    children 	
  }
}
