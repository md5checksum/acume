package com.guavus.acume.cache.utility

import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.File
import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.StringTokenizer
import java.util.TimeZone

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.MutableList
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang.StringUtils
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types._

import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.DataType
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.DimensionSet
import com.guavus.acume.cache.common.FieldType
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.MeasureSet
import com.guavus.acume.cache.core.EvictionDetails
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.disk.utility.CubeUtil
import com.guavus.acume.cache.eviction.EvictionPolicy
import com.guavus.acume.cache.gen.Acume

import javax.xml.bind.JAXBContext


/**
 * @author archit.thakur
 *
 */
object Utility extends Logging {

  def getEmptySchemaRDD(sqlContext: SQLContext, schema: StructType)= {
    
    val sparkContext = sqlContext.sparkContext
    val _$rdd = sparkContext.parallelize(1 to 1).map(x =>Row.fromSeq(Nil)).filter(x => false)
    sqlContext.applySchema(_$rdd, schema)
  }
  
  def getEmptySchemaRDD(sqlContext: SQLContext, cube: Cube) = {
    
    val sparkContext = sqlContext.sparkContext
    val _$rdd = sparkContext.parallelize(1 to 1).map(x =>Row.fromSeq(Nil)).filter(x => false)
    val cubeFieldList = cube.dimension.dimensionSet ++ cube.measure.measureSet
    val schema = cubeFieldList.map(field => { 
            StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
          })
    val latestschema = StructType(schema.+:(StructField("ts", LongType, true)))
    sqlContext.applySchema(_$rdd, latestschema)
    
  }
  
  def insertInto(sqlContext: SQLContext, schema: StructType, newrdd: SchemaRDD, tbl: String, newtbl: String) = {
    
    import sqlContext._
    sqlContext.applySchema(sqlContext.table(tbl).union(newrdd), schema).registerTempTable(newtbl)
  }
  
  def unmarshalXML(xml: String, dimensionMap : InsensitiveStringKeyHashMap[Dimension], measureMap : InsensitiveStringKeyHashMap[Measure]) = {
  
    val jc = JAXBContext.newInstance("com.guavus.acume.cache.gen")
    val unmarsh = jc.createUnmarshaller()
    val acumeCube = unmarsh.unmarshal(new FileInputStream(xml)).asInstanceOf[Acume]
    for(lx <- acumeCube.getFields().getField().toList) { 

      val info = lx.getInfo.split(",")
      if(info.length != 5)
        throw new RuntimeException("Incorrect fieldInfo in cubedefiniton.xml")
      
      val name = info(0).trim
      val datatype = DataType.getDataType(info(1).trim)
      val fitype = FieldType.getFieldType(info(2).trim)
      val functionName = info(3).trim
      info(4) = info(4).trim
      
      var defaultVal = datatype.typeString match {
        case "int" => info(4).toInt
        case "long" => info(4).toLong
               case "string" => info(4).toString
               case "float" => info(4).toFloat
               case "double" => info(4).toDouble
               case "boolean" => info(4).toBoolean
               case "short" => info(4).toShort
               case "byte" => info(4).toByte
               //case "bytebuffer" => info(4).toArray[B]
               //case "pcsa" => info(4)
               //case "null" => info(4)
               //case "binary" => info(4)
               //case "timestamp" => info(4)
               case _ => 0
      }
      
      fitype match{
        case FieldType.Dimension => 
          dimensionMap.put(name.trim, new Dimension(name, datatype, defaultVal))
        case FieldType.Measure => 
          measureMap.put(name.trim, new Measure(name, datatype, functionName, defaultVal))
      }
    }
    acumeCube
  }
  
  
  def loadXML(xml: String, dimensionMap : InsensitiveStringKeyHashMap[Dimension], measureMap : InsensitiveStringKeyHashMap[Measure],
    cubeMap : InsensitiveStringKeyHashMap[Cube], defaultPropertyMap : HashMap[String, String], cubeList : MutableList[Cube]) = { 
    
    val acumeCube = unmarshalXML(xml, dimensionMap, measureMap)
    val defaultPropertyTuple = acumeCube.getDefault.split(",").map(_.trim).map(kX => {
      val xtoken = kX.split(":")
      (xtoken(0).trim, xtoken(1).trim)
    })
        
    val defaultPropertyMap = defaultPropertyTuple.toMap
    
    val list = 
      for(c <- acumeCube.getCubes().getCube().toList) yield {
        val cubeName = c.getName().trim
        val fields = c.getFields().split(",").map(_.trim)
        val dimensionSet = scala.collection.mutable.MutableList[Dimension]()
        val measureSet = scala.collection.mutable.MutableList[Measure]()
        for(ex <- fields){
          val fieldName = ex.trim

          //only basic functions are supported as of now. 
          //Extend this to support custom udf of hive as well.
          
          dimensionMap.get(fieldName) match{
          case Some(dimension) => 
            dimensionSet.+=(dimension)
          case None =>
            measureMap.get(fieldName) match{
            case None => throw new Exception("Field not registered.")
            case Some(measure) => measureSet.+=(measure)
            }
          }
        }
        
        val _$cubeProperties = c.getProperties()
        val _$propertyMap = _$cubeProperties.split(",").map(x => {
          val i = x.indexOf(":")
          (x.substring(0, i).trim, x.substring(i+1, x.length).trim)
        })
        val propertyMap = _$propertyMap.toMap
        
        val levelpolicymap = Utility.getLevelPointMap(getProperty(propertyMap, defaultPropertyMap, ConfConstants.levelpolicymap, cubeName))
        val timeserieslevelpolicymap = Utility.getLevelPointMap(getProperty(propertyMap, defaultPropertyMap, ConfConstants.timeserieslevelpolicymap, cubeName))
        val Gnx = getProperty(propertyMap, defaultPropertyMap, ConfConstants.basegranularity, cubeName)
        val granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(Gnx).getOrElse(throw new RuntimeException("Granularity doesnot exist " + Gnx))
        val _$eviction = Class.forName(getProperty(propertyMap, defaultPropertyMap, ConfConstants.evictionpolicyforcube, cubeName)).asSubclass(classOf[EvictionPolicy])
        val cube = Cube(cubeName, DimensionSet(dimensionSet.toList), MeasureSet(measureSet.toList), granularity, true, levelpolicymap, timeserieslevelpolicymap, _$eviction)
        cubeMap.put(cubeName, cube)
        cube
      }
    cubeList.++=(list)
  }
  
  private def getProperty(propertyMap: Map[String, String], defaultPropertyMap: Map[String, String], name: String, nmCube: String) = {
    propertyMap.getOrElse(name, defaultPropertyMap.getOrElse(name, throw new RuntimeException(s"The configurtion $name should be done for cube $nmCube")))
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


//  def getLevelPointMap1(mapString: String): SortedMap[Long, Integer] = {
//    val result = new TreeMap[Long, Integer]()
//    val tok = new StringTokenizer(mapString, ";")
//    while (tok.hasMoreTokens()) {
//      val currentMapElement = tok.nextToken()
//      var gran: String = null
//      var points: Int = 0
//      gran = currentMapElement.substring(0, currentMapElement.indexOf(':'))
//      points = java.lang.Integer.valueOf(currentMapElement.substring(currentMapElement.indexOf(':') + 1))
//      val granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(gran)
//      if (granularity == null) {
//        throw new IllegalArgumentException("Unsupported Granularity  " + gran)
//      }
//      val level = granularity.getGranularity
//      result.put(level, points)
//    }
//    result
//  }
  
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
  
  def newCalendar(): Calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
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
  
  def getAllInclusiveIntervals(startTime: Long, endTime: Long, gran: Long): MutableList[Long] = {
    var _start = startTime
    val intervals = MutableList[Long]()
    val instance = newCalendar()
    while (_start <= endTime) {
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
  
  def getPreviousTimeForGranularity(time: Long, gran: Long, instance: Calendar): Long = {
    val timeGranularity = TimeGranularity.getTimeGranularity(gran).getOrElse(throw new RuntimeException)
    floorToTimezone(time - getMinTimeGran(gran), timeGranularity, instance);
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

  
 def getTimeZoneInfo(id: String, startYear: Int, endYear: Int, timezoneDBFilePath : String): TimeZoneInfo = {
    var transTimes: Array[Int] = null
    var transTypes: Array[Byte] = null
    var dst: Array[Byte] = null
    var offset: Array[Int] = null
    var idx: Array[Byte] = null
    var utcOffset = 0
    var tzname: Array[String] = null
    val f = new File(timezoneDBFilePath, id)
    val ds = new DataInputStream(new BufferedInputStream(new FileInputStream(f)))
    try {
      ds.skip(32)
      val timecnt = ds.readInt()
      val typecnt = ds.readInt()
      val charcnt = ds.readInt()
      transTimes = Array.ofDim[Int](timecnt)
      for (i <- 0 until timecnt) {
        transTimes(i) = ds.readInt()
      }
      transTypes = Array.ofDim[Byte](timecnt)
      ds.readFully(transTypes)
      offset = Array.ofDim[Int](typecnt)
      dst = Array.ofDim[Byte](typecnt)
      idx = Array.ofDim[Byte](typecnt)
      for (i <- 0 until typecnt) {
        offset(i) = ds.readInt()
        dst(i) = ds.readByte()
        idx(i) = ds.readByte()
      }
      val str = Array.ofDim[Byte](charcnt)
      ds.readFully(str)
      tzname = Array.ofDim[String](typecnt)
      for (i <- 0 until typecnt) {
        val pos = idx(i)
        var end = pos
        while (str(end) != 0) end
        tzname(i) = new String(str, pos, end - pos)
      }
      var i = transTimes.length - 1
      breakable {
      while (i > 0) {
        if (dst(transTypes(i)) == 0) {
          utcOffset = offset(transTypes(i))
          break
        }
        i -= 1
      }
      }
    } finally {
      ds.close()
    }
    val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    val rules = new java.util.ArrayList[java.util.List[String]]()
    for (i <- 0 until transTimes.length) {
      if (i > 0) {
        cal.setTimeInMillis(transTimes(i) * 1000L + offset(transTypes(i - 1)) * 1000L)
      } else {
        cal.setTimeInMillis(transTimes(i) * 1000L + utcOffset)
      }
      val year = cal.get(Calendar.YEAR)
      if (year < startYear || year > endYear) {
        // do nothing for continuing
      } else {
      val tempRule = new java.util.ArrayList[String]()
      tempRule.add(String.valueOf(year))
      tempRule.add(String.valueOf(cal.get(Calendar.DAY_OF_WEEK_IN_MONTH)))
      tempRule.add(String.valueOf(cal.get(Calendar.DAY_OF_WEEK)))
      tempRule.add(String.valueOf(cal.get(Calendar.MONTH)))
      tempRule.add(String.valueOf(cal.get(Calendar.HOUR_OF_DAY)))
      tempRule.add(String.valueOf(cal.get(Calendar.MINUTE)))
      tempRule.add(String.valueOf((offset(transTypes(i)) - utcOffset)))
      tempRule.add(String.valueOf(utcOffset))
      tempRule.add(tzname(transTypes(i)))
      rules.add(tempRule)
    }
 }
    val zone = TimeZone.getTimeZone(id)
    val zoneName = zone.getDisplayName(false, 0)
    val zoneFullName = zone.getDisplayName(false, 1)
    val dstName = zone.getDisplayName(true, 0)
    val dstFullName = zone.getDisplayName(true, 1)
    val result = new TimeZoneInfo(rules, utcOffset, id, zoneName, zoneFullName, dstName, dstFullName)
    result
    
  }

  def getTimeZoneInfo(ids: List[String], startYear: Int, endYear: Int, timezoneDbFilePath : String): List[TimeZoneInfo] = {
    val result = new ArrayBuffer[TimeZoneInfo]()
    for (id <- ids) {
      result.add(getTimeZoneInfo(id, startYear, endYear, timezoneDbFilePath))
    }
    result.toList
  }


}