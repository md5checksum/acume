package com.guavus.acume.cache.core

import java.util.Random
import scala.Array.canBuildFrom
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.MutableList
import scala.collection.mutable.SortedSet
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.disk.utility.CubeUtil
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.MetaData
import com.guavus.acume.cache.workflow.RequestType.Aggregate
import com.guavus.acume.cache.workflow.RequestType.RequestType
import com.guavus.acume.cache.workflow.RequestType.Timeseries
import com.guavus.acume.cache.common.LoadType
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.LoadType

/**
 * @author archit.thakur
 *
 */

class AcumeFlatSchemaTreeCache(keyMap: Map[String, Any], acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeTreeCache(acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy) {

  @transient val sqlContext = acumeCacheContext.cacheSqlContext
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeFlatSchemaTreeCache].getSimpleName() + "-" + cube.getAbsoluteCubeName)
  val diskUtility = DataLoader.getDataLoader(acumeCacheContext, conf, this)


  override def createTempTable(keyMap: List[Map[String, Any]], startTime: Long, endTime: Long, requestType: RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, queryOptionalParam, false)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, false)
    }
  }
  
  val concurrencyLevel = conf.get(ConfConstants.rrcacheconcurrenylevel).toInt
  val acumetreecachesize = concurrencyLevel + concurrencyLevel * (cube.diskLevelPolicyMap.map(_._2).reduce(_ + _))
  cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
    .maximumSize(acumetreecachesize).removalListener(new RemovalListener[LevelTimestamp, AcumeTreeCacheValue] {
      def onRemoval(notification: RemovalNotification[LevelTimestamp, AcumeTreeCacheValue]) {
        logger.info("Evicting timestamp {} from acume.", notification.getKey())
      }
  })
  .build(
      new CacheLoader[LevelTimestamp, AcumeTreeCacheValue]() {
        def load(key: LevelTimestamp): AcumeTreeCacheValue = {
          val output = checkIfTableAlreadyExist(key)
          if (output != null || key.loadType == LoadType.DISK) {
            if(output != null) {
            	return output
            } else {
              throw new NoDataException
            }
          } else {
            logger.info(s"Getting data from Insta for $key as it was never calculated")
          }
          //First check if point can be populated through children
          try {
        	var schema: StructType = null
        	val childrenLevel = cacheLevelPolicy.getChildrenLevel(key.level.localId)
        	val childrenAggregationLevel = cacheLevelPolicy.getAggregationLevel(childrenLevel)
        	logger.info("Children for key " + key +   " are " + childrenLevel + " - " + childrenAggregationLevel)
        	import acumeCacheContext.sqlContext._
        	var floorTime = Utility.floorFromGranularity(key.timestamp, childrenAggregationLevel)
        	var rdds = MutableList[(AcumeValue, SchemaRDD)]()
        	if(key.timestamp != floorTime) {
            val acumeValue = tryGet(new LevelTimestamp(CacheLevel.getCacheLevel(childrenLevel), floorTime, LoadType.DISK, CacheLevel.getCacheLevel(childrenAggregationLevel)))
            rdds.++=(if (acumeValue != null) {
              schema = acumeValue.getAcumeValue.measureSchemaRdd.schema
              MutableList( (acumeValue.getAcumeValue, acumeValue.getAcumeValue.measureSchemaRdd.where('ts >= key.timestamp).where('ts < Utility.getNextTimeFromGranularity(key.timestamp, childrenAggregationLevel, Utility.newCalendar))))
            } else {
              (for (child <- cacheLevelPolicy.getCombinableIntervals(floorTime, childrenAggregationLevel, childrenLevel)) yield {
                if (child >= key.timestamp) {
                  val childValue = tryGet(new LevelTimestamp(CacheLevel.getCacheLevel(childrenLevel), child, LoadType.DISK))
                  if(childValue != null) {
                  	val outputRdd = childValue.getAcumeValue.measureSchemaRdd
                    schema = outputRdd.schema
                  	(childValue.getAcumeValue, outputRdd)
                  } else {
                    throw new NoDataException()
                  }
                } else {
                  null
                }
              }).filter(_ != null)
            })
            floorTime = Utility.getNextTimeFromGranularity(floorTime, childrenAggregationLevel, Utility.newCalendar)
          }
        	
        	val endTime = Utility.getNextTimeFromGranularity(key.timestamp, key.level.localId, Utility.newCalendar)
        	while(floorTime < endTime) {
        		val tempEndTime = Utility.getNextTimeFromGranularity(floorTime, childrenAggregationLevel, Utility.newCalendar)
        		val acumeValue = tryGet(new LevelTimestamp(CacheLevel.getCacheLevel(childrenLevel), floorTime, LoadType.DISK, CacheLevel.getCacheLevel(childrenAggregationLevel)))
              rdds.++=(if (acumeValue != null) {
                schema = acumeValue.getAcumeValue.measureSchemaRdd.schema
                if (tempEndTime > endTime) {
                  MutableList( (acumeValue.getAcumeValue, acumeValue.getAcumeValue.measureSchemaRdd.where('ts < endTime)) )
                } else {
                  MutableList( (acumeValue.getAcumeValue, acumeValue.getAcumeValue.measureSchemaRdd) )
                }
              } else {
                (for (child <- cacheLevelPolicy.getCombinableIntervals(floorTime, childrenAggregationLevel, childrenLevel)) yield {
                  if (child < endTime) {
                    val childValue = tryGet(new LevelTimestamp(CacheLevel.getCacheLevel(childrenLevel), child, LoadType.DISK))
                    if(childValue == null) {
                      throw new Exception()
                    }
                    val outputRdd = childValue.getAcumeValue.measureSchemaRdd
                    schema = outputRdd.schema
                    (childValue.getAcumeValue, outputRdd)
                  } else {
                    null
                  }
                }).filter(_ != null)
              })
              floorTime = tempEndTime
        	}
          if (schema != null) {
            return populateParentPointFromChildren(key, rdds, schema)
          }
          
        } catch {
          case e: Exception => logger.info(s"Couldnt populate data for $key as all children are not present.")
        }

        if (key.loadType == LoadType.Insta) {
          logger.info(s"Getting data from Insta for $key as all children are not present ")
          return getDataFromBackend(key);
        } else {
            throw new NoDataException
        }
      }
  });
  
  def populateParentPointFromChildren(key : LevelTimestamp, acumeValRdds : Seq[(AcumeValue, SchemaRDD)], schema : StructType) : AcumeTreeCacheValue = {

    logger.info("Populating parent point from children for key " + key)
    val emptyRdd = Utility.getEmptySchemaRDD(sqlContext, schema)

    val _tableName = cube.cubeName + key.level.toString + key.timestamp.toString

    val value = mergeChildPoints(emptyRdd, acumeValRdds.map(x => x._2))
    
    //aggregate over measures after merging child points
    val (selectDimensions, selectMeasures, groupBy) = CubeUtil.getDimensionsAggregateMeasuresGroupBy(cube)

    val tempTable = _tableName + "Temp"
    value.registerTempTable(tempTable)
    AcumeCacheContextTrait.setInstaTempTable(tempTable)
    val timestamp = s"${key.timestamp}L"
    val parentRdd = acumeCacheContext.sqlContext.sql(s"select $timestamp as ts " + (if(!selectDimensions.isEmpty) s", $selectDimensions " else "") + (if(!selectMeasures.isEmpty) s", $selectMeasures" else "") + s" from $tempTable " + groupBy)
    return new AcumeFlatSchemaCacheValue(new AcumeInMemoryValue(key, cube, parentRdd, acumeValRdds), acumeCacheContext)
  }
  
  override def mergeChildPoints(emptyRdd : SchemaRDD, rdds : Seq[SchemaRDD]) : SchemaRDD = {
    rdds.reduce(_.unionAll(_))
  }

  private def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf("_"))

  override def createTempTableAndMetadata(keyMap: List[Map[String, Any]], startTime: Long, endTime: Long, requestType: RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData = {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, queryOptionalParam, true)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, true)
    }
  }

  override def getAggregateCachePoints(
      startTime: Long,
      endTime: Long,
      tableName: String,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean) : (Seq[SchemaRDD], List[Long]) = {

    val duration = endTime - startTime
    val timestampMap : Option[MutableMap[Long, MutableList[(Long, Long)]]] = queryOptionalParam match {
      case Some(param) =>
        if (param.getTimeSeriesGranularity() != 0) {
          val level = param.getTimeSeriesGranularity
          val startTimeCeiling = cacheLevelPolicy.getCeilingToLevel(startTime, level)
          val endTimeFloor = cacheLevelPolicy.getFloorToLevel(endTime, level)
          Some(MutableMap(level -> MutableList((startTimeCeiling, endTimeFloor))))
        } else None
      case None => None
    }
    val levelTimestampMap = timestampMap.getOrElse(cacheLevelPolicy.getRequiredIntervals1(startTime, endTime))
    getCachePointsForIntervals(levelTimestampMap, tableName, isMetaData)
  }

  override def getCachePoints(
      startTime: Long,
      endTime: Long,
      tableName: String,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) = {

    val baseLevel = cube.baseGran.getGranularity
    val level =
      queryOptionalParam match {
        case Some(param) =>
          if (param.getTimeSeriesGranularity() != 0) {
            var level = Math.max(baseLevel, param.getTimeSeriesGranularity());
            val variableRetentionMap = getVariableRetentionMap
            if (!variableRetentionMap.contains(new Level(level))) {
              val headMap = variableRetentionMap.filterKeys(_.level < level);
              if (headMap.size == 0) {
                throw new IllegalArgumentException("Wrong granularity " + level + " passed in request which is not present in variableRetentionMap ");
              }
              level = headMap.lastKey.level
            }
            level
          } else
            Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, acumeCacheContext.getLastBinPersistedTime(cube.binsource)))
        case None =>
          Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, acumeCacheContext.getLastBinPersistedTime(cube.binsource)))
      }

    val startTimeCeiling = cacheLevelPolicy.getCeilingToLevel(startTime, level)
    val endTimeFloor = cacheLevelPolicy.getFloorToLevel(endTime, level)
    // val list = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)

    if (startTimeCeiling < endTimeFloor) {
      val intervals: MutableMap[Long, MutableList[(Long, Long)]] = MutableMap(level -> MutableList((startTimeCeiling, endTimeFloor)))
      getCachePointsForIntervals(intervals, tableName, isMetaData)

    } else {
      (Seq(Utility.getEmptySchemaRDD(acumeCacheContext.sqlContext, cube)), Nil)
    }
  }

  private def createTableForAggregate(startTime: Long, endTime: Long, tableName: String, queryOptionalParam: Option[QueryOptionalParam], isMetaData: Boolean): MetaData = {

    val duration = endTime - startTime
    val timestampMap : Option[MutableMap[Long, MutableList[(Long, Long)]]] = queryOptionalParam match {
        case Some(param) =>
          if (param.getTimeSeriesGranularity() != 0) {
            val level = param.getTimeSeriesGranularity
            val startTimeCeiling = cacheLevelPolicy.getCeilingToLevel(startTime, level)
            val endTimeFloor = cacheLevelPolicy.getFloorToLevel(endTime, level)
            Some(MutableMap(level -> MutableList((startTimeCeiling, endTimeFloor))))
          } else None
        case None => None
      }
    val levelTimestampMap = timestampMap.getOrElse(cacheLevelPolicy.getRequiredIntervals1(startTime, endTime))
    buildTableForIntervals(levelTimestampMap, tableName, isMetaData)
  }
  
  private def createTableForTimeseries(startTime: Long, endTime: Long, tableName: String, queryOptionalParam: Option[QueryOptionalParam], isMetaData: Boolean): MetaData = {

    val baseLevel = cube.baseGran.getGranularity
    val level =
      queryOptionalParam match {
        case Some(param) =>
          if (param.getTimeSeriesGranularity() != 0) {
            var level = Math.max(baseLevel, param.getTimeSeriesGranularity());
            val variableRetentionMap = getVariableRetentionMap
            if (!variableRetentionMap.contains(new Level(level))) {
              val headMap = variableRetentionMap.filterKeys(_.level < level);
              if (headMap.size == 0) {
                throw new IllegalArgumentException("Wrong granularity " + level + " passed in request which is not present in variableRetentionMap ");
              }
              level = headMap.lastKey.level
            }
            level
          } else
            Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, acumeCacheContext.getLastBinPersistedTime(cube.binsource)))
        case None =>
          Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, acumeCacheContext.getLastBinPersistedTime(cube.binsource)))
      }

    val startTimeCeiling = cacheLevelPolicy.getCeilingToLevel(startTime, level)
    val endTimeFloor = cacheLevelPolicy.getFloorToLevel(endTime, level)
    val list = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
    
    if (!list.isEmpty) {
      val intervals: MutableMap[Long, MutableList[(Long, Long)]] = MutableMap(level -> MutableList((startTimeCeiling, endTimeFloor)))
      buildTableForIntervals(intervals, tableName, isMetaData)
    } else {
      acumeCacheContext.cacheSqlContext.registerRDDAsTable(Utility.getEmptySchemaRDD(acumeCacheContext.sqlContext, cube), tableName)
      MetaData(-1, Nil)
    }
  }

  private def getVariableRetentionMap: SortedMap[Level, Int] = {
    val cubelocal = cube.levelPolicyMap
    SortedMap[Level, Int]() ++ cubelocal
  }

  override def getDataFromBackend(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
   // val _tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    import acumeCacheContext.sqlContext._
    val cacheLevel = levelTimestamp.level
    val startTime = levelTimestamp.timestamp
    val endTime = Utility.getNextTimeFromGranularity(startTime, cacheLevel.localId, Utility.newCalendar)
    val diskloaded = diskUtility.loadData(keyMap, cube, startTime, endTime, cacheLevel.localId)
    val processedDiskLoaded = processBackendData(diskloaded)
    
    val _tableNameTemp = cube.getAbsoluteCubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString + "_temp"
    processedDiskLoaded.registerTempTable(_tableNameTemp)
    AcumeCacheContextTrait.setInstaTempTable(_tableNameTemp)
    val timestamp = s"${levelTimestamp.timestamp}L"
    val measureSet = (CubeUtil.getDimensionSet(cube) ++ CubeUtil.getMeasureSet(cube)).map(_.getName).mkString(",")
    val cachePoint = sqlContext.sql(s"select $timestamp as ts, $measureSet from " + _tableNameTemp)
    new AcumeFlatSchemaCacheValue(new AcumeInMemoryValue(levelTimestamp, cube, cachePoint), acumeCacheContext)
  }
  
  def processBackendData(rdd: SchemaRDD) : SchemaRDD = {
    rdd
  }

  private def getUniqueRandomeNo: String = System.currentTimeMillis() + "" + Math.abs(new Random().nextInt)

  private def buildTableForIntervals(levelTimestampMap: MutableMap[Long, MutableList[(Long, Long)]], tableName: String, isMetaData: Boolean): MetaData = {
    import acumeCacheContext.sqlContext._
    logger.info("Total timestamps are : {}", cachePointToTable.asMap().keySet())
    val finalTimestamps: MutableList[Long] = MutableList[Long]()
    var finalSchema = null.asInstanceOf[StructType]
    val x = getCubeName(tableName)
    val levelTime = for((level, timestamps) <- levelTimestampMap) yield {
      val aggregationlevel = cacheLevelPolicy.getAggregationLevel(level)
      (for((startTime, endTime) <- timestamps) yield {
        var timestamp = Utility.floorFromGranularity(startTime, aggregationlevel)
        val finalRdds = ArrayBuffer[SchemaRDD]()
        while(timestamp < endTime) {
          
          val (aggregatedTimestamp, acumeValue) = 
            if(level == aggregationlevel) {
              val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), timestamp, LoadType.Insta, CacheLevel.getCacheLevel(aggregationlevel))
              (aggregatedTimestamp, get(aggregatedTimestamp))
            } else {
              val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), timestamp, LoadType.DISK, CacheLevel.getCacheLevel(aggregationlevel))
              (aggregatedTimestamp, tryGet(aggregatedTimestamp))
            }
          	val (tempStart, tempEnd) = (Math.max(startTime, timestamp), Math.min(endTime, Utility.getNextTimeFromGranularity(timestamp, aggregationlevel, Utility.newCalendar)))
        	finalTimestamps.++=(Utility.getAllIntervals(tempStart, tempEnd, level))
        	val acumeValues = if(acumeValue == null) {
        	  logger.info("Table not found for timestamp {}", aggregatedTimestamp)
        	  val intervals = Utility.getAllIntervals(tempStart, tempEnd, level)
        	  for(interval <- intervals) yield {
        	    val levelTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), interval, CacheLevel.getCacheLevel(level))
                // logger.info("Selecting table with timestamp {} for interval {}, {}", levelTimestamp, startTime.toString, endTime.toString)
        	    val innerAcumeValue = cachePointToTable.get(levelTimestamp).getAcumeValue.measureSchemaRdd
//        	    populateParent(levelTimestamp.level.localId, levelTimestamp.timestamp)
        	    combineLevels(levelTimestamp.level.localId, levelTimestamp.timestamp)
        	    innerAcumeValue
        	  }
        	} else {

              // logger.info("Selecting table with timestamp {}", aggregatedTimestamp)
        	  if(level == aggregationlevel) {
        	    Seq(acumeValue.getAcumeValue.measureSchemaRdd)
        	  } else {
        		Seq(acumeValue.getAcumeValue.measureSchemaRdd.where('ts >= startTime).where('ts < endTime))
        	  }
        	}
        	timestamp = Utility.getNextTimeFromGranularity(timestamp, aggregationlevel, Utility.newCalendar)
        	finalRdds ++= acumeValues
        }
        finalRdds
      }).flatten

    }
    logger.info("Timestamps in final output are {}", finalTimestamps)
    import scala.collection.JavaConversions._
    
    if (!levelTime.isEmpty) {
      val schemarddlist = levelTime.flatten
      val dataloadedrdd = if(schemarddlist.size == 1) {
        val emptyRdd = sqlContext.applySchema(sparkContext.emptyRDD[Row], schemarddlist.toList.get(0).schema)
        val list = List(emptyRdd) ++ schemarddlist.toList
        mergePathRdds(list.map(_.asInstanceOf[SchemaRDD]).toIterable)
      } else {
        mergePathRdds(schemarddlist)
      }
      val baseMeasureSetTable = cube.cubeName + "MeasureSet" + getUniqueRandomeNo
      val joinDimMeasureTableName = baseMeasureSetTable + getUniqueRandomeNo
      val _$acumecache = dataloadedrdd
      if (logger.isTraceEnabled)
        _$acumecache.collect.map(x => logger.trace(x.toString))
      _$acumecache.registerTempTable(tableName)
      AcumeCacheContextTrait.setQueryTable(tableName)
    }
    val klist = finalTimestamps.toList
    MetaData(-1, klist)
  }

  private def getCachePointsForIntervals(
      levelTimestampMap: MutableMap[Long, MutableList[(Long, Long)]],
      tableName: String,
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) = {
    import acumeCacheContext.sqlContext._
    logger.info("Total timestamps are : {}", cachePointToTable.asMap().keySet())
    val cal = Utility.newCalendar
    val finalTimestamps: MutableList[Long] = MutableList[Long]()
    var finalSchema = null.asInstanceOf[StructType]
    val x = getCubeName(tableName)
    val finalRdds = ArrayBuffer[SchemaRDD]()
    val iterator = levelTimestampMap.iterator
    while(iterator.hasNext) {
      val (level, timestamps) = iterator.next
        val aggregationlevel = cacheLevelPolicy.getAggregationLevel(level)
        var i = 0
        while (i < timestamps.length) {
          val (startTime, endTime) = timestamps(i)
          i += 1
          var timestamp = Utility.floorFromGranularity(startTime, aggregationlevel)
          while (timestamp < endTime) {

            val (aggregatedTimestamp, acumeValue) =
              if (level == aggregationlevel) {
                val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), timestamp, LoadType.Insta, CacheLevel.getCacheLevel(aggregationlevel))
                (aggregatedTimestamp, get(aggregatedTimestamp))
              } else {
                val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), timestamp, LoadType.DISK, CacheLevel.getCacheLevel(aggregationlevel))
                (aggregatedTimestamp, tryGet(aggregatedTimestamp))
              }
            val (tempStart, tempEnd) = (Math.max(startTime, timestamp), Math.min(endTime, Utility.getNextTimeFromGranularity(timestamp, aggregationlevel, cal)))
            finalTimestamps.++=(Utility.getAllIntervals(tempStart, tempEnd, level))
            val acumeValues = if (acumeValue == null) {
              logger.info("Table not found for timestamp {}", aggregatedTimestamp)
              val intervals = Utility.getAllIntervals(tempStart, tempEnd, level)
              for (interval <- intervals) yield {
                val levelTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), interval, CacheLevel.getCacheLevel(level))
                // logger.info("Selecting table with timestamp {} for interval {}, {}", levelTimestamp, startTime.toString, endTime.toString)
                val innerAcumeValue = cachePointToTable.get(levelTimestamp).getAcumeValue.measureSchemaRdd
                //              populateParent(levelTimestamp.level.localId, levelTimestamp.timestamp)
                combineLevels(levelTimestamp.level.localId, levelTimestamp.timestamp)
                innerAcumeValue
              }
            } else {

              // logger.info("Selecting table with timestamp {}", aggregatedTimestamp)
              if (level == aggregationlevel) {
                Seq(acumeValue.getAcumeValue.measureSchemaRdd)
              } else {
                Seq(acumeValue.getAcumeValue.measureSchemaRdd.where('ts >= startTime).where('ts < endTime))
              }
            }
            timestamp = Utility.getNextTimeFromGranularity(timestamp, aggregationlevel, cal)
            finalRdds ++= acumeValues
          }
        }
    }

    logger.info("Timestamps in final output are {}", finalTimestamps)

    (finalRdds.toList, finalTimestamps.toList)

  }

  /* private def getCachePointsForIntervals(
      levelTimestampMap: MutableMap[Long, MutableList[(Long, Long)]],
      tableName: String,
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) = {
    import acumeCacheContext.sqlContext._
    logger.info("Total timestamps are : {}", cachePointToTable.asMap().keySet())
    val cal = Utility.newCalendar
    val finalTimestamps: MutableList[Long] = MutableList[Long]()
    var finalSchema = null.asInstanceOf[StructType]
    val x = getCubeName(tableName)
    val levelTime = for((level, timestamps) <- levelTimestampMap) yield {
      val aggregationlevel = cacheLevelPolicy.getAggregationLevel(level)
      (for((startTime, endTime) <- timestamps) yield {
        var timestamp = Utility.floorFromGranularity(startTime, aggregationlevel)
        val finalRdds = ArrayBuffer[SchemaRDD]()
        while(timestamp < endTime) {

          val (aggregatedTimestamp, acumeValue) =
            if(level == aggregationlevel) {
              val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), timestamp, LoadType.Insta, CacheLevel.getCacheLevel(aggregationlevel))
              (aggregatedTimestamp, get(aggregatedTimestamp))
            } else {
              val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), timestamp, LoadType.DISK, CacheLevel.getCacheLevel(aggregationlevel))
              (aggregatedTimestamp, tryGet(aggregatedTimestamp))
            }
          val (tempStart, tempEnd) = (Math.max(startTime, timestamp), Math.min(endTime, Utility.getNextTimeFromGranularity(timestamp, aggregationlevel, cal)))
          finalTimestamps.++=(Utility.getAllIntervals(tempStart, tempEnd, level))
          val acumeValues = if(acumeValue == null) {
            logger.info("Table not found for timestamp {}", aggregatedTimestamp)
            val intervals = Utility.getAllIntervals(tempStart, tempEnd, level)
            for(interval <- intervals) yield {
              val levelTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(level), interval, CacheLevel.getCacheLevel(level))
              // logger.info("Selecting table with timestamp {} for interval {}, {}", levelTimestamp, startTime.toString, endTime.toString)
              val innerAcumeValue = cachePointToTable.get(levelTimestamp).getAcumeValue.measureSchemaRdd
              //        	    populateParent(levelTimestamp.level.localId, levelTimestamp.timestamp)
              combineLevels(levelTimestamp.level.localId, levelTimestamp.timestamp)
              innerAcumeValue
            }
          } else {

            // logger.info("Selecting table with timestamp {}", aggregatedTimestamp)
            if(level == aggregationlevel) {
              Seq(acumeValue.getAcumeValue.measureSchemaRdd)
            } else {
              Seq(acumeValue.getAcumeValue.measureSchemaRdd.where('ts >= startTime).where('ts < endTime))
            }
          }
          timestamp = Utility.getNextTimeFromGranularity(timestamp, aggregationlevel, cal)
          finalRdds ++= acumeValues
        }
        finalRdds
      }).flatten

    }
    logger.info("Timestamps in final output are {}", finalTimestamps)

    (levelTime.flatten.toList, finalTimestamps.toList)

  } */

}
class NoDataException extends Exception
