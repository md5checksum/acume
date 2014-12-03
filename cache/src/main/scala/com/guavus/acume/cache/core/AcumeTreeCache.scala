
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
import scala.collection.JavaConversions._

/**
 * @author archit.thakur
 *
 */
private [cache] class AcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache(acumeCacheContext, conf, cube) {

  val dimensionTable: DimensionTable = DimensionTable("AcumeCacheGlobalDimensionTable" + cube.cubeName)
  val diskUtility = DataLoader.getDataLoader(acumeCacheContext, conf, this)
  
  override def createTempTable(startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    val x = requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, false)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, false)
    }
     for(cacheE: LevelTimestamp <- cachePointToTable.asMap.keySet.toSet) {
 
  print("Eviction Testing--> "+cacheE.level +"::"+cacheE.timestamp)
 
 }
     x
  }

  val concurrencyLevel = conf.get(ConfConstants.rrcacheconcurrenylevel).toInt
  val acumetreecachesize = concurrencyLevel + concurrencyLevel * (cube.levelPolicyMap.map(_._2).reduce(_+_))
  private val cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
  .maximumSize(acumetreecachesize).removalListener(new RemovalListener[LevelTimestamp, AcumeTreeCacheValue] {
	  def onRemoval(notification : RemovalNotification[LevelTimestamp, AcumeTreeCacheValue]) {
	    acumeCacheContext.sqlContext.uncacheTable(notification.getValue().measuretableName)
	  }
  })
  .build(
      new CacheLoader[LevelTimestamp, AcumeTreeCacheValue]() {
        def load(key: LevelTimestamp): AcumeTreeCacheValue = {
          return getData(key);
        }
      });
  
  private val list = new MutableList[AcumeCacheObserver]
  
  override def getCacheCollection = cachePointToTable
  private def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf("_"))
  
  override def createTempTableAndMetadata(startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData = {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, true)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, true)
    }
  }
  
  def newObserverAddition(acumeCacheObserver: AcumeCacheObserver) = {
    
    list.+=(acumeCacheObserver)
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
        Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, conf.get(ConfConstants.lastbinpersistedtime).toLong))
    }
    
    val startTimeCeiling = cacheLevelPolicy.getCeilingToLevel(startTime, level)
    val endTimeFloor = cacheLevelPolicy.getFloorToLevel(endTime, level)
    val list = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
    if(!list.isEmpty) { 
      
      val intervals: MutableMap[Long, MutableList[Long]] = MutableMap(level -> list)
      buildTableForIntervals(intervals, tableName, isMetaData)
    }
    else { 
      
      Utility.getEmptySchemaRDD(acumeCacheContext.sqlContext, cube).registerTempTable(tableName)
      MetaData(Nil)
    }
  }
  
  private def getVariableRetentionMap: SortedMap[Long, Int] = {
    val contextCollection = acumeCacheContext.vrmap
    SortedMap[Long, Int]() ++ contextCollection
  }
  
  private def getData(levelTimestamp: LevelTimestamp) = {

    import acumeCacheContext.sqlContext._
    val cacheLevel = levelTimestamp.level
    val diskloaded = diskUtility.loadData(cube, levelTimestamp, dimensionTable)
    val _$dataset = diskloaded._1
    val _$dimt = diskloaded._2
    val _tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    val value = acumeCacheContext.sqlContext.applySchema(_$dataset, _$dataset.schema)
    value.registerTempTable(_tableName)
    cacheTable(_tableName) 
    AcumeTreeCacheValue(_$dimt, _tableName, value)
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
        val _$diskread = acumeCacheContext.sqlContext.applySchema(diskread, finalSchema)
        
        _$diskread
      }
      timeIterated
    }
    if(!levelTime.isEmpty) {
    val schemarddlist = levelTime.flatten
    val dataloadedrdd = applySchema(schemarddlist.reduce(_.unionAll(_)), finalSchema)
    val baseMeasureSetTable = cube.cubeName + "MeasureSet" + getUniqueRandomeNo
    val joinDimMeasureTableName = baseMeasureSetTable + getUniqueRandomeNo
    dataloadedrdd.registerTempTable(baseMeasureSetTable)
    AcumeCacheUtility.dMJoin(acumeCacheContext.sqlContext, dimensionTable.tblnm, baseMeasureSetTable, joinDimMeasureTableName)
    val _$acumecache = AcumeCacheUtility.getSchemaRDD(acumeCacheContext, cube, joinDimMeasureTableName)
    acumeCacheContext.sqlContext.applySchema(_$acumecache, _$acumecache.schema).registerTempTable(tableName)
    }
    val klist = timestamps.toList
    MetaData(klist)
  }
}