package com.guavus.acume.cache.utility

import scala.collection.mutable.MutableList
import scala.collection.mutable.{Map => MutableMap}
import java.util.TimeZone
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang.StringUtils
import com.guavus.acume.cache.core.EvictionDetails
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.Logging
import java.util.StringTokenizer
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

object Utility extends Logging {

  def getEmptySchemaRDD(sqlContext: SQLContext, schema: StructType)= {
    
    val sparkContext = sqlContext.sparkContext
    val _$rdd = sparkContext.parallelize(1 to 1).map(x =>Row.fromSeq(Nil)).filter(x => false)
    sqlContext.applySchema(_$rdd, schema)
  }
  
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
                val retentionMap = getLevelPointMap(retentionMapString)
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
  
  def getTimeZone(id: String): TimeZone = {
    val tz: TimeZone = 
      try {
        new ZoneInfo(id)
      } catch {
      case e: Exception=> TimeZone.getTimeZone(id)
      }
    tz
  }

  def humanReadableTimeStamp(timestampInSeconds: Long): String = {
    val dateFormat = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss z")
    dateFormat.setTimeZone(getTimeZone("UTC"))
    dateFormat.format(new Date(timestampInSeconds * 1000))
  }

  def humanReadableTimeInterval(startTime: Long, endTime: Long): String = humanReadableTime(endTime - startTime)

  def humanReadableTime(time: Long): String = {
    val sb = new StringBuilder()
    val minutes = time / (1000 * 60)
    if (minutes != 0) sb.append(minutes).append(" minutes ")
    val seconds = (time / 1000) % 60
    if (seconds != 0) sb.append(seconds).append(" seconds ")
    val millis = time % 1000
    sb.append(millis).append(" milliseconds")
    sb.toString
  }

  def floorFromGranularity(time: Long, gran: Long): Long = {
    val timeGranularity = TimeGranularity.getTimeGranularity(gran).getOrElse(throw new RuntimeException)
    floorToTimeZone(time, timeGranularity)
  }

  def floorFromGranularityAndTimeZone(time: Long, gran: Long, timezone: TimeZone): Long = {
    if (timezone != null) {
      val timeGranularity = TimeGranularity.getTimeGranularity(gran).getOrElse(throw new RuntimeException)
      val instance = newCalendar(timezone)
      floorToTimezone(time, timeGranularity, instance)
    } else {
      floorFromGranularity(time, gran)
    }
  }

  def floorToTimeZone(time: Long, timeGrnaularity: TimeGranularity): Long = {
    val instance = newCalendar(TimeZone.getTimeZone("GMT"))
    floorToTimezone(time, timeGrnaularity, instance)
  }

  def floorToTimezone(time: Long, timeGrnaularity: TimeGranularity, instance: Calendar): Long = {
    instance.setTimeInMillis(time * 1000)
    timeGrnaularity match {
      case MONTH => 
        var minimumDatePrevMonth = instance.getActualMinimum(Calendar.DAY_OF_MONTH)
        instance.set(Calendar.DAY_OF_MONTH, minimumDatePrevMonth)
        instance.set(Calendar.HOUR_OF_DAY, 0)
        instance.set(Calendar.MINUTE, 0)

      case DAY => 
        instance.set(Calendar.HOUR_OF_DAY, 0)
        instance.set(Calendar.MINUTE, 0)

      case HALF_DAY => 
        var hour = ((instance.get(Calendar.HOUR_OF_DAY) / TimeGranularity.HALF_DAY.getDurationInHour) * 
          TimeGranularity.HALF_DAY.getDurationInHour).toInt
        instance.set(Calendar.MINUTE, 0)
        instance.set(Calendar.HOUR_OF_DAY, hour)

      case FOUR_HOUR => 
        var hour = ((instance.get(Calendar.HOUR_OF_DAY) / TimeGranularity.FOUR_HOUR.getDurationInHour) * 
          TimeGranularity.FOUR_HOUR.getDurationInHour).toInt
        instance.set(Calendar.MINUTE, 0)
        instance.set(Calendar.HOUR_OF_DAY, hour)

      case THREE_HOUR => 
        var hour = ((instance.get(Calendar.HOUR_OF_DAY) / TimeGranularity.THREE_HOUR.getDurationInHour) * 
          TimeGranularity.THREE_HOUR.getDurationInHour).toInt
        instance.set(Calendar.MINUTE, 0)
        instance.set(Calendar.HOUR_OF_DAY, hour)

      case HOUR => instance.add(Calendar.MINUTE, -1 * instance.get(Calendar.MINUTE))
      case FIVE_MINUTE => 
        var minute = ((instance.get(Calendar.MINUTE) / TimeGranularity.FIVE_MINUTE.getDurationInMinutes) * 
          TimeGranularity.FIVE_MINUTE.getDurationInMinutes).toInt
        instance.add(Calendar.MINUTE, -1 * (instance.get(Calendar.MINUTE) - minute))

      case FIFTEEN_MINUTE => 
        var minute = ((instance.get(Calendar.MINUTE) / 
          TimeGranularity.FIFTEEN_MINUTE.getDurationInMinutes) * 
          TimeGranularity.FIFTEEN_MINUTE.getDurationInMinutes).toInt
        instance.add(Calendar.MINUTE, -1 * (instance.get(Calendar.MINUTE) - minute))

      case ONE_MINUTE => 
      case TWO_DAYS | THREE_DAYS => 
        var days = daysFromReference(instance, instance.getTimeZone)
        if (days < 0) days *= -1
        var offset = days % timeGrnaularity.getDurationInDay.toInt
        instance.add(Calendar.DAY_OF_MONTH, -1 * offset)
        instance.set(Calendar.MINUTE, 0)
        instance.set(Calendar.HOUR_OF_DAY, 0)

      case WEEK => 
        instance.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
        instance.set(Calendar.MINUTE, 0)
        instance.set(Calendar.HOUR_OF_DAY, 0)

      case _ => throw new IllegalArgumentException("This " + timeGrnaularity + 
        " floor is not supported. Make changes to support it")
    }
    instance.add(Calendar.SECOND, -1 * instance.get(Calendar.SECOND))
    instance.getTimeInMillis / 1000
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
  
  def newCalendar(): Calendar = Calendar.getInstance
  def newCalendar(timezone: TimeZone): Calendar = Calendar.getInstance(timezone)
  
  def getAllIntervals(startTime: Long, endTime: Long, gran: Long): MutableList[Long] = {
    var _start = startTime
    val intervals = MutableList[Long]()
    val instance = newCalendar()
    while (_start < endTime) {
      intervals.+=(_start)
      _start = getNextTimeFromGranularity(_start, gran, instance)
    }
    intervals
  }

  def getAllIntervalsAtTZ(startTime: Long, endTime: Long, gran: Long, timezone: TimeZone): MutableList[Long] = {
    var _z = startTime
    if (timezone == null) 
      getAllIntervals(_z, endTime, gran)
    else{
      val intervals = MutableList[Long]()
      val instance = newCalendar(timezone)
      while (_z < endTime) {
        intervals.add(_z)
        _z = getNextTimeFromGranularity(_z, gran, instance)
      }
      intervals
    }
  }
  
  def ceiling(time: Long, timeGranularity: TimeGranularity): Long = {
    ceiling(time, timeGranularity.getGranularity)
  }

  def ceiling(time: Long, round: Long): Long = {
    if (time % round != 0) { 
      return ((time / round) + 1) * round
    }
    time
  }
  
  def floor(time: Long, round: Long): Long = (time / round) * round

  def floor(time: Long, timeGranularity: TimeGranularity): Long = {
    floor(time, timeGranularity.getGranularity)
  }

  def ceiling(time: Int, timeGranularity: TimeGranularity): Int = {
    val result = ceiling(time.toLong, timeGranularity.getGranularity)
    if (result > java.lang.Integer.MAX_VALUE) return java.lang.Integer.MAX_VALUE
    result.toInt
  }
  
  def ceilingFromGranularity(time: Long, gran: Long): Long = {
    val timeGranularity = TimeGranularity.getTimeGranularity(gran).getOrElse(throw new RuntimeException("TimeGranularity does not exist for gran" + gran))
    ceilingToTimeZone(time, timeGranularity)
  }

  def ceilingToTimeZone(time: Long, timeGrnaularity: TimeGranularity): Long = {
    val instance = newCalendar(TimeZone.getTimeZone("GMT"))
    ceilingToTimezone(time, timeGrnaularity, instance)
  }
  
  def getMinimumHour(instance: Calendar): Int = {
    val cal = instance.clone().asInstanceOf[Calendar]
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.get(Calendar.HOUR_OF_DAY)
  }
  
  def daysFromReference(startDate: Calendar, timezone: TimeZone): Int = {
    val endDate = Calendar.getInstance(timezone)
    endDate.set(Calendar.YEAR, 2000)
    endDate.set(Calendar.MONTH, 0)
    endDate.set(Calendar.DAY_OF_MONTH, 1)
    endDate.set(Calendar.HOUR_OF_DAY, 0)
    endDate.set(Calendar.MINUTE, 0)
    endDate.set(Calendar.SECOND, 0)
    endDate.set(Calendar.MILLISECOND, 0)
    val endTime = endDate.getTimeInMillis
    val startTime = startDate.getTimeInMillis
    var days = ((startTime - endTime) / (1000 * 60 * 60 * 24)).toInt
    endDate.add(Calendar.DAY_OF_YEAR, days)
    if (endDate.getTimeInMillis == startDate.getTimeInMillis) {
      return -1 * days
    }
    if (endDate.getTimeInMillis < startDate.getTimeInMillis) {
      while (endDate.getTimeInMillis < startDate.getTimeInMillis) {
        endDate.add(Calendar.DAY_OF_MONTH, 1)
        days += 1
      }
      if (endDate.getTimeInMillis == startDate.getTimeInMillis) {
        -1 * days
      } else {
        days - 1
      }
    } else {
      while (endDate.getTimeInMillis > startDate.getTimeInMillis) {
        endDate.add(Calendar.DAY_OF_MONTH, -1)
        days -= 1
      }
      if (endDate.getTimeInMillis == startDate.getTimeInMillis) {
        -1 * days
      } else {
        days
      }
    }
  }

  def getMinTimeGran(gran: Long): Long = {
    if (gran == TimeGranularity.HOUR.getGranularity) {
      return TimeGranularity.HOUR.getGranularity
    }
    TimeGranularity.ONE_MINUTE.getGranularity
  }
  
  def getNextTimeFromGranularity(time: Long, gran: Long, instance: Calendar): Long = {
    val timeGranularity = TimeGranularity.getTimeGranularity(gran).getOrElse(throw new RuntimeException)
    ceilingToTimezone(time + getMinTimeGran(gran), timeGranularity, instance)
  }
  
  def ceilingToTimezone(time: Long, timeGrnaularity: TimeGranularity, instance: Calendar): Long = {
    instance.setTimeInMillis(time * 1000)
    timeGrnaularity match {
      case MONTH => if (instance.get(Calendar.DATE) > 1 || instance.get(Calendar.HOUR_OF_DAY) > 0 || 
        instance.get(Calendar.MINUTE) > 0 || 
        instance.get(Calendar.SECOND) > 0) {
        val minimumDatePrevMonth = instance.getActualMinimum(Calendar.DAY_OF_MONTH)
        instance.set(Calendar.DAY_OF_MONTH, minimumDatePrevMonth)
        instance.add(Calendar.MONTH, 1)
        instance.set(Calendar.HOUR_OF_DAY, 0)
        instance.set(Calendar.MINUTE, 0)
      }
      case DAY => if (instance.get(Calendar.HOUR_OF_DAY) > getMinimumHour(instance) || 
        instance.get(Calendar.MINUTE) > 0 || 
        instance.get(Calendar.SECOND) > 0) {
        instance.add(Calendar.DAY_OF_MONTH, 1)
        instance.set(Calendar.HOUR_OF_DAY, 0)
        instance.set(Calendar.MINUTE, 0)
      }
      case HALF_DAY | FOUR_HOUR | THREE_HOUR => 
        var hour = timeGrnaularity.getDurationInHour
        var hourOfDay = instance.get(Calendar.HOUR_OF_DAY)
        if (hourOfDay % hour != 0 || instance.get(Calendar.MINUTE) > 0 || 
          instance.get(Calendar.SECOND) > 0) {
          hourOfDay = ((hourOfDay / hour) + 1) * hour toInt
        }
        instance.set(Calendar.HOUR_OF_DAY, hourOfDay.toInt)
        instance.set(Calendar.MINUTE, 0)

      case HOUR => if (instance.get(Calendar.MINUTE) > 0 || instance.get(Calendar.SECOND) > 0) {
        instance.add(Calendar.HOUR_OF_DAY, 1)
        instance.add(Calendar.MINUTE, -1 * instance.get(Calendar.MINUTE))
      }
      case FIVE_MINUTE => instance.setTimeInMillis(ceiling(instance.getTimeInMillis / 1000, TimeGranularity.FIVE_MINUTE) * 
        1000)
      case FIFTEEN_MINUTE => instance.setTimeInMillis(ceiling(instance.getTimeInMillis / 1000, TimeGranularity.FIFTEEN_MINUTE) * 
        1000)
      case ONE_MINUTE => if (instance.get(Calendar.SECOND) > 0) {
        instance.add(Calendar.MINUTE, 1)
      }
      case TWO_DAYS | THREE_DAYS => 
        var days = daysFromReference(instance, instance.getTimeZone)
        if (days < 0 && days % timeGrnaularity.getDurationInDay != 0) {
          days *= -1
        }
        if (days > 0) {
          val offset = timeGrnaularity.getDurationInDay.toInt - days % timeGrnaularity.getDurationInDay.toInt
          instance.add(Calendar.DAY_OF_MONTH, offset)
          instance.set(Calendar.MINUTE, 0)
          instance.set(Calendar.HOUR_OF_DAY, 0)
        }

      case WEEK => if (instance.get(Calendar.DAY_OF_WEEK) != Calendar.SUNDAY || 
        instance.get(Calendar.HOUR_OF_DAY) > 0 || 
        instance.get(Calendar.MINUTE) > 0 || 
        instance.get(Calendar.SECOND) > 0) {
        instance.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
        instance.add(Calendar.DAY_OF_MONTH, Calendar.SATURDAY)
        instance.set(Calendar.MINUTE, 0)
        instance.set(Calendar.HOUR_OF_DAY, 0)
      }
      case _ => throw new IllegalArgumentException("This " + timeGrnaularity + 
        "ceiling is not supported. Make changes to support it")
    }
    instance.add(Calendar.SECOND, -1 * instance.get(Calendar.SECOND))
    instance.getTimeInMillis / 1000
  }


}