package com.guavus.acume.cache.core

import java.io.Serializable
import java.util.Set
import java.util.TreeSet
import Interval._
import scala.collection.JavaConversions._
import com.guavus.acume.cache.utility.Utility

/**
 * @author archit.thakur
 *
 */
object Interval {

  private class TimeEvent(var time: Long, var isStart: Boolean) extends Comparable[TimeEvent] {

    override def compareTo(o: TimeEvent): Int = {
      if (time < o.time) {
        return -1
      } else if (time == o.time) {
        if (isStart != o.isStart) {
          return if (!isStart) -1 else 1
        }
      } else {
        return 1
      }
      -1
    }

    override def equals(e: Any): Boolean = {
      val event = e.asInstanceOf[TimeEvent]
      this.compareTo(event) == 0
    }
  }

  def mergeInterval(inputInterval: Set[Interval]): Set[Interval] = {
    val outputInterval = new TreeSet[Interval]()
    var stTime = -1l
    var etTime = -1l
    for (interval <- inputInterval) {
      if (stTime == -1 && etTime == -1) {
        stTime = interval.startTime
        etTime = interval.endTime
      }
      else { 
        if (interval.startTime <= etTime) {
          etTime = Math.max(etTime, interval.endTime)
        } else if (interval.startTime > etTime) {
          outputInterval.add(new Interval(stTime, etTime))
          stTime = interval.startTime
          etTime = interval.endTime
        }
      }
    }
    if (stTime != -1 || etTime != -1) {
      outputInterval.add(new Interval(stTime, etTime))
    }
    outputInterval
  }

  def isInInterval(inputInterval: Set[Interval], time: Long): Boolean = {
    for (interval <- inputInterval
        if (interval.startTime <= time && interval.endTime > time)) {
        return true
    }
    false
  }

  def isInInterval(schedulerBinTimeInterval: Interval, time: java.lang.Long): java.lang.Boolean = {
    if (schedulerBinTimeInterval == null) {
      return false
    }
    if (schedulerBinTimeInterval.startTime > time || schedulerBinTimeInterval.endTime <= time) {
      false
    } else {
      true
    }
  }

  def getIntervalIntersection(finalInterval: Set[Interval], curInterval: Set[Interval]): Set[Interval] = {
    var _finalInterval = mergeInterval(finalInterval)
    val _curInterval = mergeInterval(curInterval)
    val events = new TreeSet[TimeEvent]()
    for (interval <- _finalInterval) {
      events.add(new TimeEvent(interval.startTime, true))
      events.add(new TimeEvent(interval.endTime, false))
    }
    for (interval <- _curInterval) {
      events.add(new TimeEvent(interval.startTime, true))
      events.add(new TimeEvent(interval.endTime, false))
    }
    var overlapCount = 0
    var startTime = -1l
    _finalInterval = new TreeSet[Interval]()
    for (event <- events) {
      if (event.isStart) {
        startTime = event.time
        overlapCount += 1
      } else {
        if (overlapCount >= 2) {
          _finalInterval.add(new Interval(startTime, event.time))
        }
        overlapCount -= 1
      }
    }
    _finalInterval
  }

  def addInterval(persistBinTimeInterval: Set[Interval], newInterval: Interval) {
    if (persistBinTimeInterval != null && !persistBinTimeInterval.isEmpty) {
      persistBinTimeInterval.add(newInterval)
      val intervals = mergeInterval(persistBinTimeInterval)
      persistBinTimeInterval.clear()
      persistBinTimeInterval.addAll(intervals)
    } else if (persistBinTimeInterval.isEmpty) {
      persistBinTimeInterval.add(new Interval(newInterval.getStartTime, newInterval.getEndTime))
    }
  }

  def addInterval(persistBinTimeInterval: Interval, newInterval: Interval): Interval = {
    var _persistBinTimeInterval = persistBinTimeInterval
    if (_persistBinTimeInterval != null) {
      val interval = _persistBinTimeInterval
      if (newInterval != null) {
        val startTime = Math.min(newInterval.getStartTime, interval.getStartTime)
        val endTime = Math.max(newInterval.getEndTime, interval.getEndTime)
        interval.startTime = startTime
        interval.endTime = endTime
      }
    } else if (_persistBinTimeInterval == null) {
      _persistBinTimeInterval = new Interval(newInterval.getStartTime, newInterval.getEndTime)
    }
    _persistBinTimeInterval
  }
}

class Interval extends Comparable[Interval] with Serializable {

  var startTime: Long = _
  var endTime: Long = _
  var presentTime: Long = _
  var granularity: Long = _

  def setStartTime(startTime: Long) {
    this.startTime = startTime
  }

  def setEndTime(endTime: Long) {
    this.endTime = endTime
  }

  def setPresentTime(presentTime: Long) {
    this.presentTime = presentTime
  }

  def setGranularity(granularity: Long) {
    this.granularity = granularity
  }

  
  def this(startTime: Long, 
      endTime: Long, 
      presentTime: Long, 
      granularity: Long) {
    this()
    this.startTime = startTime
    this.endTime = endTime
    this.presentTime = presentTime
    this.granularity = granularity
  }
  
  def this(startTime: Long, endTime: Long, presentTime: Long) {
    this(startTime, endTime, presentTime, 0)
  }
  
  def this(startTime: Long, endTime: Long) {
    this(startTime, endTime, startTime)
  }

  def getGranularity(): Long = granularity
  def getStartTime(): Long = startTime
  def getEndTime(): Long = endTime
  def getPresentTime(): Long = presentTime

  def isEndTimeValid(endTime: Long): Boolean = {
    if (this.endTime >= endTime) {
      return true
    }
    false
  }

  def isStartTimeValid(startTime: Long): Boolean = {
    if (this.endTime >= endTime) {
      return true
    }
    false
  }

  def isValidInInterval(intervals: Set[Interval]): Boolean = {
    intervals.find(interval => isValidInInterval(interval.getStartTime, interval.getEndTime))
      .map(_ => true)
      .getOrElse(false)
  }

  def isValidInInterval(interval: Interval): Boolean = {
    isValidInInterval(interval.getStartTime, interval.getEndTime)
  }

  def isValidInInterval(startTime: Long, endTime: Long): Boolean = {
    if ((this.endTime <= endTime) && (this.startTime >= startTime)) {
      return true
    }
    false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (endTime ^ (endTime >>> 32)).toInt
    result = prime * result + (startTime ^ (startTime >>> 32)).toInt
    result = prime * result + (presentTime ^ (presentTime >>> 32)).toInt
    result = prime * result + (granularity ^ (granularity >>> 32)).toInt
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj) return true
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[Interval]
    if (endTime != other.endTime) return false
    if (startTime != other.startTime) return false
    if (presentTime != other.presentTime) return false
    if (granularity != other.granularity) return false
    true
  }

  override def toString(): String = {
    val builder = new StringBuilder()
    builder.append("Interval [startTime=")
    builder.append(Utility.humanReadableTimeStamp(startTime))
    builder.append(", endTime=")
    builder.append(Utility.humanReadableTimeStamp(endTime))
    builder.append(", presentTime=")
    builder.append(Utility.humanReadableTimeStamp(presentTime))
    builder.append(", granularity=")
    builder.append(granularity)
    builder.toString
  }

  override def compareTo(o: Interval): Int = {
    if (this == o) 0 
    else {
      if (this.startTime > o.startTime) 1
      else -1
    }
  }
}
