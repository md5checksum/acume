package com.guavus.acume.cache.util


import scala.collection.mutable.MutableList
import scala.collection.mutable.{Map => MutableMap}
import java.util.TimeZone
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang.StringUtils
import com.guavus.acume.cache.core.EvictionDetails
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.acume.cache.common.AcumeConstants
import scala.collection.SortedMap

import com.guavus.acume.cache.util.Utility;
import com.guavus.acume.cache.core.TimeGranularity
import org.apache.spark.Logging
import java.util.StringTokenizer

object Utility12345 extends Logging {

  def createEvictionDetailsMapFromFile(): MutableMap[String, EvictionDetails] = {
    val evictionDetailsMap = MutableMap[String, EvictionDetails]()
    try {
      val properties: PropertiesConfiguration = new PropertiesConfiguration()
      properties.setDelimiterParsingDisabled(true)
      properties.load("evictiondetails.properties")
      
      val keySet = properties.getKeys
      while (keySet.hasNext) {
        val key = keySet.next().asInstanceOf[String]
        val value = Option(properties.getString(key))
        value match{
          case None => 
          case Some(value) => {
            
          val valuesArr = value.split(AcumeConstants.LINE_DELIMITED)
          if (valuesArr.length == 1 && !value.contains(AcumeConstants.LINE)) {
            try {
              val memoryEvictionCount = Integer.parseInt(valuesArr(0))
              val evictionDetails = new EvictionDetails()
              evictionDetails.setMemoryEvictionThresholdCount(memoryEvictionCount)
              evictionDetailsMap += key -> evictionDetails
            } catch {
              case e: Exception => {
                logError("Error " + e + " in parseEvictionDetailsMapFromFile while parsing " + key)
              }
            }
          } else if (valuesArr.length > 1 || (valuesArr.length == 1 && value.contains("|"))) {
            val policyName = valuesArr(0)
            val retentionMapString = 
              if (valuesArr.length > 1) {
                valuesArr(1)
              } else ""
            try {
              val evictionDetails = new EvictionDetails()
              if (StringUtils.isNotBlank(policyName)) {
                Class.forName(policyName)
                evictionDetails.setEvictionPolicyName(policyName)
              }
              if (StringUtils.isNotBlank(retentionMapString)) {
                val retentionMap = Utility12345.getLevelPointMap(retentionMapString)
                evictionDetails.setVariableRetentionMap(retentionMap)
              }
              
              evictionDetailsMap.put(key, evictionDetails)
            } catch {
              case e: Exception => {
                logError("Error " + e + " in parseEvictionDetailsMapFromFile while parsing " + key)
              }
            }
          } else {
            logError("Error in parseEvictionDetailsMapFromFile while parsing " + key)
          } } 	
        }
      }  
    } catch {
      case e: Throwable => {
        logError("Error " + e + " in parseEvictionDetailsMapFromFile...")
        e.printStackTrace()
      }
    }
    evictionDetailsMap
  }
  
  
  def getLevelPointMap(mapString: String): Map[Long, Int] = {
    val result = MutableMap[Long, Int]()
    val tok = new StringTokenizer(mapString, ";")
    while (tok.hasMoreTokens()) {
      val currentMapElement = tok.nextToken()
      val token = currentMapElement.split(":")
      val gran: String = token(0)
      val nmx = token(1).toInt
      val granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(gran) match{
        case None => throw new IllegalArgumentException("Unsupported Granularity  " + gran)
        case Some(value) => value
      }
      val level = granularity.getGranularity
      result.put(level, nmx)
    }
    result.toMap
  }
  
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