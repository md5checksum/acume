package com.guavus.acume.cache.utility

import java.text.SimpleDateFormat
import scala.util.control.Breaks._
import java.util.Calendar
import java.util.Date
import java.util.StringTokenizer
import java.util.TimeZone
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.MutableList
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang.StringUtils
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.core.EvictionDetails
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.cache.disk.utility.CubeUtil
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.workflow.CubeKey
import com.guavus.acume.cache.gen.Acume
import scala.collection.SortedMap
import scala.collection.mutable.HashMap
import javax.xml.bind.JAXBContext
import scala.collection.immutable.TreeMap
import java.io.DataInputStream
import java.io.File
import java.io.BufferedInputStream
import java.io.FileInputStream
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.guavus.acume.cache.common.AcumeCacheConf
import scala.collection.mutable.ArrayBuffer
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.DataType
import com.guavus.acume.cache.common.FieldType
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.MeasureSet
import com.guavus.acume.cache.common.DimensionSet
import com.guavus.acume.cache.eviction.EvictionPolicy
import com.guavus.acume.cache.core.AcumeCacheType
import com.guavus.rubix.query.remote.flex.TimeZoneInfo
import com.guavus.acume.cache.core.Level
import org.apache.spark.SparkContext
import java.io.OutputStream
import java.io.InputStream
import java.util.Collection
import java.io.Closeable
import org.apache.hadoop.fs.Path
import com.guavus.acume.cache.common.CacheLevel
import CacheLevel._
import com.google.common.collect.Iterables
import acume.exception.AcumeException

/**
 * @author archit.thakur
 *
 */

object Utility extends Logging {
  
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
      
  var calendar : Calendar = null
  def init(conf : AcumeCacheConf) {
	 calendar = newCalendar(TimeZone.getTimeZone(conf.get(ConfConstants.timezone)))
  }
  
  def newCalendar() = calendar.clone().asInstanceOf[Calendar]

  
  def getCausalChain(throwable : Throwable) = {
	    var tempThrowable = throwable
		val causes = new java.util.LinkedHashSet[Throwable]()
	    while (tempThrowable != null && !causes.contains(tempThrowable)) {
	      causes.add(tempThrowable);
	      tempThrowable = tempThrowable.getCause();
	    }
	    causes
	  }
  
  def throwIfRubixException(t : Throwable) {
		val reItr = Iterables.filter(Utility.getCausalChain(t), classOf[AcumeException]).iterator();
		if(reItr.hasNext())
			throw reItr.next();
	}
  
  def getEmptySchemaRDD(sqlContext: SQLContext, schema: StructType)= {
    val rdd = sqlContext.sparkContext.emptyRDD[Row]
    sqlContext.applySchema(rdd, schema)
  }
  
  def withDummyCallSite[T](sc: SparkContext)(body: => T): T = {
    val oldShortCallSite = sc.getLocalProperty(SHORT_FORM)
    val oldLongCallSite = sc.getLocalProperty(LONG_FORM)
    try {
      sc.setLocalProperty(SHORT_FORM, "")
      sc.setLocalProperty(LONG_FORM, "")
      body
    } finally {
      // Restore the old ones here
      sc.setLocalProperty(SHORT_FORM, oldShortCallSite)
      sc.setLocalProperty(LONG_FORM, oldLongCallSite)
    }
  }
  
  def getStartTimeFromLevel(endTime : Long, granularity : Long, points : Int) : Long = {
			var rangeEndTime = Utility.floorFromGranularity(endTime, granularity);
			var rangeStartTime : Long = 0 
			val cal = Utility.newCalendar();
			if(granularity == TimeGranularity.MONTH.getGranularity()){
				cal.setTimeInMillis(rangeEndTime * 1000);
				cal.add(Calendar.MONTH, -1 * points);
				rangeStartTime = cal.getTimeInMillis() / 1000;
			}
			else if(granularity == TimeGranularity.WEEK.getGranularity()) {
				cal.setTimeInMillis(rangeEndTime * 1000);
				cal.add(Calendar.WEEK_OF_MONTH, -1 * points);
				rangeStartTime = cal.getTimeInMillis() / 1000;
			}else if(granularity == TimeGranularity.DAY.getGranularity()) {
				cal.setTimeInMillis(rangeEndTime * 1000);
				cal.add(Calendar.DAY_OF_MONTH, -1 * points);
				rangeStartTime = cal.getTimeInMillis() / 1000;
			}
			else{
				rangeStartTime = rangeEndTime - points*granularity;
			}
			return rangeStartTime;
	}
  
  def getEmptySchemaRDD(sqlContext: SQLContext, cube: Cube) = {
    
    val sparkContext = sqlContext.sparkContext
    val _$rdd = sparkContext.emptyRDD[Row]
    val cubeFieldList = cube.dimension.dimensionSet ++ cube.measure.measureSet
    val schema = cubeFieldList.map(field => { 
            StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
          })
    val latestschema = StructType(schema.+:(StructField("ts", LongType, true)))
    sqlContext.applySchema(_$rdd, latestschema)
    
  }
  
  def insertInto(sqlContext: SQLContext, schema: StructType, newrdd: SchemaRDD, tbl: String, newtbl: String) = {
    
    import sqlContext._
    sqlContext.table(tbl).unionAll(newrdd).registerTempTable(newtbl)
  }
  
//  def createEvictionDetailsMapFromFile(): MutableMap[String, EvictionDetails] = { }
  
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
    val instance = newCalendar
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

  
  def unmarshalXML(xml: String, dimensionMap : InsensitiveStringKeyHashMap[Dimension], measureMap : InsensitiveStringKeyHashMap[Measure]) = {
  
    val jc = JAXBContext.newInstance("com.guavus.acume.cache.gen")
    val unmarsh = jc.createUnmarshaller()
    val acumeCube = unmarsh.unmarshal(new FileInputStream(xml)).asInstanceOf[Acume]
    for(lx <- acumeCube.getFields().getField().toList) { 

      val info = lx.getInfo.split(",")
      var baseFieldName = lx.getBaseFieldName();
      if(info.length != 5)
        throw new RuntimeException("Incorrect fieldInfo in cubedefiniton.xml")
      
      val name = info(0).trim
      if(baseFieldName == null)
        baseFieldName = name
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
          dimensionMap.put(name.trim, new Dimension(name, baseFieldName, datatype, defaultVal))
        case FieldType.Measure => 
          measureMap.put(name.trim, new Measure(name, baseFieldName, datatype, functionName, defaultVal))
      }
    }
    acumeCube
  }
  
  def loadXML(conf: AcumeCacheConf, dimensionMap : InsensitiveStringKeyHashMap[Dimension], measureMap : InsensitiveStringKeyHashMap[Measure],
    cubeMap : HashMap[CubeKey, Cube], cubeList : MutableList[Cube]) = { 
    
    val xml: String = conf.get(ConfConstants.businesscubexml) 
    val globalbinsource: String = conf.get(ConfConstants.acumecorebinsource)
    val acumeCube = unmarshalXML(xml, dimensionMap, measureMap)

    val list = 
      for(c <- acumeCube.getCubes().getCube().toList) yield {
        val cubeinfo = c.getInfo().trim.split(",")
        val (cubeName, cubebinsource) = 
          if(cubeinfo.length == 1) {
            val _$binning = globalbinsource
            if(_$binning.isEmpty) throw new RuntimeException("binsource for the cube " + cubeinfo + " cannot be determined.")
            (cubeinfo(0).trim, _$binning)
          }
          else if(cubeinfo.length == 2)
            (cubeinfo(0).trim, cubeinfo(1).trim)
          else
            throw new RuntimeException(s"Cube.Info is wrongly specified for cube $cubeinfo")
        
        if(cubeMap.contains(CubeKey(cubeName, cubebinsource))) {
          throw new RuntimeException("Xml contains more than one cube with same CubeKey(cubename + cubebinsource).")
        }
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
        val propertyMap = scala.collection.mutable.HashMap(_$propertyMap.toSeq: _*)
        
        //getSingle entity keys from xml
        val singleEntityKeys = c.getSingleEntityKeys()
        var singleEntityKeysMap : Map[String, String] = if (singleEntityKeys != null) {
          singleEntityKeys.split(",").map(x => {
            val i = x.indexOf(":")
            (x.substring(0, i).trim, x.substring(i + 1, x.length).trim)
          }).toMap
        } else {
          Map[String, String]()
        }
        
        val levelPolicyString = getProperty(propertyMap, ConfConstants.levelpolicymap, ConfConstants.acumecorelevelmap, conf, cubeName)
        val levelpolicymap = levelPolicyString.split("\\|")
        val inMemoryPolicyMap = Utility.getLevelPointMap(levelpolicymap(0))
        val diskLevelPolicyMap = 
          if(levelpolicymap.size == 1) {
          	inMemoryPolicyMap
        	} else {
        	  Utility.getLevelPointMap(levelpolicymap(1))
        	}
        
        if(!PropertyValidator.validateRetentionMap(Some(levelPolicyString), ConfConstants.acumecorelevelmap)) {
          throw new RuntimeException(ConfConstants.acumecorelevelmap + " is not configured correctly")
        }
        val timeserieslevelpolicymap = Utility.getLevelPointMap(getProperty(propertyMap, ConfConstants.timeserieslevelpolicymap, ConfConstants.acumecoretimeserieslevelmap, conf, cubeName)).map(x =>x._1.level -> x._2)

        val Gnx = getProperty(propertyMap, ConfConstants.basegranularity, ConfConstants.acumeglobalbasegranularity, conf, cubeName)
        val granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(Gnx).getOrElse(throw new RuntimeException("Granularity doesnot exist " + Gnx))
        val _$eviction = Class.forName(getProperty(propertyMap, ConfConstants.evictionpolicyforcube, ConfConstants.acumeglobalevictionpolicycube, conf, cubeName)).asSubclass(classOf[EvictionPolicy])
        val schemaType = AcumeCacheType.getAcumeCacheType(getProperty(propertyMap, "cacheType", ConfConstants.acumeCacheDefaultType, conf, cubeName))
        val cube = Cube(cubeName, cubebinsource, DimensionSet(dimensionSet.toList), MeasureSet(measureSet.toList), singleEntityKeysMap, granularity, true, inMemoryPolicyMap, diskLevelPolicyMap, timeserieslevelpolicymap, _$eviction, schemaType, propertyMap.toMap)
        cubeMap.put(CubeKey(cubeName, cubebinsource), cube)
        cube
      }
    cubeList.++=(list)
  }
  
   private def getProperty(propertyMap: scala.collection.mutable.HashMap[String, String], name: String, globalname: String, conf: AcumeCacheConf, gnmCube: String) = {
    propertyMap.getOrElseUpdate(name, conf.get(globalname, throw new RuntimeException(s"The configurtion $name should be done for cube $gnmCube")))
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

  def getPriority(timeStamp: Long, level: Long, aggregationLevel: Long, variableRetentionMap: Map[Level, Int], lastBinTime : Long): Int = {
    if (!variableRetentionMap.contains(new Level(level))) return 0
    val numPoints = variableRetentionMap.get(new Level(level)).getOrElse(throw new RuntimeException("Level not in VariableRetentionMap."))
    val rangeStarTime = getRangeStartTime(lastBinTime, level, numPoints)
    var timeStampTobeChecked = timeStamp
    if(aggregationLevel != level) {
      // This is a combined point
      // Check if the last child of this combined point is evictable or not
      timeStampTobeChecked = Utility.getPreviousTimeForGranularity(Utility.getNextTimeFromGranularity(timeStamp, aggregationLevel, Utility.newCalendar()), level, Utility.newCalendar())
    }
    if(timeStampTobeChecked >= rangeStarTime) 1 else 0
  }

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
  
  def getLevelPointMap(mapString: String): Map[Level, Int] = {
    val result = MutableMap[Level, Int]()
    val tok = new StringTokenizer(mapString, ";")
    while (tok.hasMoreTokens()) {
      val currentMapElement = tok.nextToken()
      val token = currentMapElement.split(":")
      val gran: String = token(0)
      val aggregationGran = if(token.length == 3) {
    	  token(2)
      } else {
        token(0)
      }
      val nmx = token(1).toInt
      val granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(gran) match{
        case None => throw new IllegalArgumentException("Unsupported Granularity  " + gran)
        case Some(value) => value
      }
      
      val aggregationGranularity = TimeGranularity.getTimeGranularityForVariableRetentionName(aggregationGran) match{
        case None => throw new IllegalArgumentException("Unsupported Granularity  " + aggregationGran)
        case Some(value) => value
      } 
      val level = granularity.getGranularity
      result.put(new Level(level, aggregationGranularity.getGranularity), nmx)
    }
    print(result)
    result.toMap
  }
  
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
    val instance = newCalendar
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
        val pos:Int = idx(i)
        var end:Int = pos
        while (str(end) != 0) end += 1
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
    val cal = newCalendar
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

  def isCause( expected : Class[_ <: Throwable],
			 exc : Throwable) : Boolean = {
		 expected.isInstance(exc) || (exc != null && isCause(expected, exc.getCause()));
	}
  
    
   /**
   * Copies passed inputstream to passed outputstream.
   * @param input
   * @param output
   * @throws IOException
   */
  def copyStream(input: InputStream, output: OutputStream) {
    val buffer = Array.ofDim[Byte](1024)
    var bytesRead: Int = input.read(buffer)
    while (bytesRead != -1) {
      output.write(buffer, 0, bytesRead)
      bytesRead = input.read(buffer)
    }
  }
  
  def closeStream(s: Closeable) {
    if (s != null) s.close()
  }
  
  def getTimeInHumanReadableForm(time: Long, timeZone: String): String = {
    val calendar = Calendar.getInstance(Utility.getTimeZone(timeZone))
    calendar.setTimeInMillis(time * 1000)
    val formatter = new SimpleDateFormat("MMM dd EEE yyyy HH:mm z")
    formatter.setTimeZone(TimeZone.getTimeZone(timeZone))
    formatter.format(calendar.getTime)
  }

  def getTimeInHumanReadableForm(time: Long, timeZone: String, calendar: Calendar): String = {
    calendar.setTimeInMillis(time * 1000)
    val formatter = new SimpleDateFormat("MMM dd EEE yyyy HH:mm z")
    formatter.setTimeZone(TimeZone.getTimeZone(timeZone))
    formatter.format(calendar.getTime)
  }
  
  def getCurrentDateInHumanReadableForm(): String = {
    val calendar = Calendar.getInstance
    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss")
    formatter.format(calendar.getTime)
  }

  def getCalendar(timeZone: String): Calendar = {
    val calendar = Calendar.getInstance(Utility.getTimeZone(timeZone))
    calendar
  }
  
  def isNullOrEmpty[T <: Collection[_]](t: T): Boolean = t == null || t.isEmpty

  def isNullOrEmpty[K, V](map: Map[K, V]): Boolean = map == null || map.isEmpty

  def isNullOrEmpty(array: Array[Long]): Boolean = array == null || (array.length == 0)
  
  def deleteDirectory(dir : String, acumeContext : AcumeCacheContextTrait) {
    logDebug("Deleting directory " + dir)
    val path = new Path(dir)
    val fs = path.getFileSystem(acumeContext.cacheSqlContext.sparkContext.hadoopConfiguration)
    fs.delete(path, true)
  }

  def isPathExisting(path : Path, acumeContext : AcumeCacheContextTrait) : Boolean = {
    logDebug("Checking if path exists => " + path)
    val fs = path.getFileSystem(acumeContext.cacheSqlContext.sparkContext.hadoopConfiguration)
    return fs.exists(path)
  }
  
  def isDiskWriteComplete(diskDirectory : String, acumeContext : AcumeCacheContextTrait) : Boolean = {
    val path =  new Path(diskDirectory + File.separator + "_SUCCESS")
    isPathExisting(path, acumeContext)
  }
  
  def getDiskBaseDirectory(acumeContext : AcumeCacheContextTrait) = {
    var diskBaseDirectory = acumeContext.cacheConf.get(ConfConstants.cacheBaseDirectory) + File.separator + acumeContext.cacheSqlContext.sparkContext.getConf.get("spark.app.name") 
    diskBaseDirectory = diskBaseDirectory + "-" + acumeContext.cacheConf.get(ConfConstants.cacheDirectory)
    diskBaseDirectory
  }
  def getCubeBaseDirectory(acumeContext : AcumeCacheContextTrait, cube : Cube) : String = {
    var cubeBaseDirectory = getDiskBaseDirectory(acumeContext) + File.separator + cube.binsource + File.separator + cube.cubeName
    cubeBaseDirectory
  }

  def getlevelDirectoryName(level: CacheLevel, aggregationLevel: CacheLevel) : String = {
    new Level(level.localId, aggregationLevel.localId).toDirectoryName
  }
  
  def getDiskDirectoryForPoint(acumeContext : AcumeCacheContextTrait, cube : Cube, levelTimestamp : LevelTimestamp) : String = {
    var diskDirectoryForPoints = getCubeBaseDirectory(acumeContext, cube)
    diskDirectoryForPoints = diskDirectoryForPoints + File.separator + getlevelDirectoryName(levelTimestamp.level, levelTimestamp.aggregationLevel)
    diskDirectoryForPoints = diskDirectoryForPoints + File.separator + levelTimestamp.timestamp
    diskDirectoryForPoints
  }
  
}
