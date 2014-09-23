package com.guavus.acume.util

import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList
import java.util.TimeZone

object Utility12345 {

  def getAllIntervals(startTime: Long, endTime: Long, gran: Long): MutableList[Long] = {
    var _start = startTime
    val intervals = MutableList[Long]()
    val instance = Utility.newCalendar()
    while (_start < endTime) {
      intervals.add(startTime)
      _start = Utility.getNextTimeFromGranularity(startTime, gran, instance)
    }
    intervals
  }

  def getAllIntervalsAtTZ(startTime: Long, endTime: Long, gran: Long, timezone: TimeZone): MutableList[Long] = {
    var _z = startTime
    if (timezone == null) 
      getAllIntervals(startTime, endTime, gran)
    else{
      val intervals = MutableList[Long]()
      val instance = Utility.newCalendar(timezone)
      while (_z < endTime) {
        intervals.add(startTime)
        _z = Utility.getNextTimeFromGranularity(startTime, gran, instance)
      }
      intervals
    }
  }
}