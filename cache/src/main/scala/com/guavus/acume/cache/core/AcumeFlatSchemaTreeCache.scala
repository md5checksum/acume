
package com.guavus.acume.cache.core

import scala.Array.canBuildFrom
import scala.collection.immutable.SortedMap
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.MutableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.StructType
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.DimensionTable
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.MetaData
import com.guavus.acume.cache.workflow.RequestType.Aggregate
import com.guavus.acume.cache.workflow.RequestType.Timeseries
import java.util.Observer
import java.util.Random
import com.guavus.acume.cache.workflow.RequestType.RequestType
import org.apache.spark.sql.SchemaRDD
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import com.guavus.acume.cache.disk.utility.CubeUtil
import org.apache.spark.sql.StructField
import com.guavus.acume.cache.common.ConversionToSpark
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.AccumulatorParam
import java.util.Arrays
import com.guavus.acume.cache.common.LevelTimestamp

/**
 * @author archit.thakur
 *
 */
private [cache] class AcumeFlatSchemaTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {

  @transient val sqlContext = acumeCacheContext.cacheSqlContext
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeFlatSchemaTreeCache])
  val diskUtility = DataLoader.getDataLoader(acumeCacheContext, conf, this)

  override def createTempTable(keyMap : Map[String, Any], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, false)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, false)
    }
  }

  val concurrencyLevel = conf.get(ConfConstants.rrcacheconcurrenylevel).toInt
  val acumetreecachesize = concurrencyLevel + concurrencyLevel * (cube.levelPolicyMap.map(_._2).reduce(_+_))
  cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
  .maximumSize(acumetreecachesize).removalListener(new RemovalListener[LevelTimestamp, AcumeTreeCacheValue] {
	  def onRemoval(notification : RemovalNotification[LevelTimestamp, AcumeTreeCacheValue]) {
	    acumeCacheContext.sqlContext.uncacheTable(notification.getValue().measuretableName)
	  }
  })
  .build(
      new CacheLoader[LevelTimestamp, AcumeTreeCacheValue]() {
        def load(key: LevelTimestamp): AcumeTreeCacheValue = {
          return getDataFromBackend(key)
        }
      });
  
  private def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf("_"))
  
  override def createTempTableAndMetadata(keyMap : Map[String, Any], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData = {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, true)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, true)
    }
  }
  
  def notifyObserverList = {
    
    list.foreach(_.update(this, conf))
  }
  
  private def createTableForAggregate(startTime: Long, endTime: Long, tableName: String, isMetaData: Boolean): MetaData = {
    
    val duration = endTime - startTime
    val levelTimestampMap = cacheLevelPolicy.getRequiredIntervals(startTime, endTime)
    buildTableForIntervals(levelTimestampMap, tableName, isMetaData)
  }
  
  private def createTableForTimeseries(startTime: Long, endTime: Long, tableName: String, queryOptionalParam: Option[QueryOptionalParam], isMetaData: Boolean): MetaData = {
    
    val baseLevel = cube.baseGran.getGranularity
    val level = 
      queryOptionalParam match{
      case Some(param) =>
        if(param.getTimeSeriesGranularity() != null) {
          var level = Math.max(baseLevel, param.getTimeSeriesGranularity());
			val variableRetentionMap = getVariableRetentionMap
			if(!variableRetentionMap.contains(level)) {
				val headMap = variableRetentionMap.filterKeys(_<level);
				if(headMap.size == 0) {
					throw new IllegalArgumentException("Wrong granularity " + level + " passed in request which is not present in variableRetentionMap ");
				}
				level = headMap.lastKey
			}
			level
        }
        else 
          0
      case None => 
        Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, acumeCacheContext.getLastBinPersistedTime(acumeCacheContext.cacheConf.get(ConfConstants.acumecorebinsource))))
    }
    
    val startTimeCeiling = cacheLevelPolicy.getCeilingToLevel(startTime, level)
    val endTimeFloor = cacheLevelPolicy.getFloorToLevel(endTime, level)
    val list = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
    if(!list.isEmpty) { 
      
      val intervals: MutableMap[Long, MutableList[Long]] = MutableMap(level -> list)
      buildTableForIntervals(intervals, tableName, isMetaData)
    }
    else { 
      
      acumeCacheContext.cacheSqlContext.registerRDDAsTable(Utility.getEmptySchemaRDD(acumeCacheContext.sqlContext, cube), tableName)
      MetaData(-1, Nil)
    }
  }
  
  private def getVariableRetentionMap: SortedMap[Long, Int] = {
    val cubelocal = cube.levelPolicyMap
    SortedMap[Long, Int]() ++ cubelocal
  }
  
  def _getDataFromBackend(pointRdd : SchemaRDD, levelTimestamp: LevelTimestamp) : AcumeTreeCacheValue= {
	val _tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    if(pointRdd != null) {
      return new AcumeTreeCacheValue(null, _tableName, pointRdd)
    }

    import acumeCacheContext.sqlContext._
    val cacheLevel = levelTimestamp.level
    val diskloaded = diskUtility.loadData(cube, levelTimestamp)
    val _tableNameTemp = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString + "_temp"
    diskloaded.registerTempTable(_tableName + "_temp")
    val timestamp = levelTimestamp.timestamp
    val measureSet = (CubeUtil.getMeasureSet(cube) ++ CubeUtil.getDimensionSet(cube)).map(_.getName).mkString(",")
    val cachePoint = sqlContext.sql(s"select $timestamp as ts, $measureSet from " + _tableNameTemp)
    cachePoint.registerTempTable(_tableName)
    cacheTable(_tableName) 
    AcumeTreeCacheValue(null, _tableName, cachePoint)
  }

  private def getUniqueRandomeNo: String = System.currentTimeMillis() + "" + Math.abs(new Random().nextInt)
  
  private def buildTableForIntervals(levelTimestampMap: MutableMap[Long, MutableList[Long]], tableName: String, isMetaData: Boolean): MetaData = {
    import acumeCacheContext.sqlContext._
    val timestamps: MutableList[Long] = MutableList[Long]()
    var finalSchema = null.asInstanceOf[StructType]
    val x = getCubeName(tableName)
    val levelTime = for(levelTsMapEntry <- levelTimestampMap) yield {
      val (level, ts) = levelTsMapEntry
      val cachelevel = CacheLevel.getCacheLevel(level)
      val timeIterated = for(item <- ts) yield {
        timestamps.+=(item)
        val levelTimestamp = LevelTimestamp(cachelevel, item)
        val acumeTreeCacheValue = cachePointToTable.get(levelTimestamp)
        val diskread = acumeTreeCacheValue.measureschemardd
        notifyObserverList
        finalSchema = diskread.schema
        val _$diskread = diskread        
        _$diskread
      }
      timeIterated
    }
    if(!levelTime.isEmpty) {
    val schemarddlist = levelTime.flatten
    val dataloadedrdd = schemarddlist.reduce(_.unionAll(_))
    val baseMeasureSetTable = cube.cubeName + "MeasureSet" + getUniqueRandomeNo
    val joinDimMeasureTableName = baseMeasureSetTable + getUniqueRandomeNo
    dataloadedrdd.registerTempTable(joinDimMeasureTableName)
    val _$acumecache = table(joinDimMeasureTableName)
    if(logger.isTraceEnabled)
      _$acumecache.collect.map(x=>logger.trace(x.toString))
    _$acumecache.registerTempTable(tableName)
    }
    val klist = timestamps.toList
    MetaData(-1, klist)
  }
}