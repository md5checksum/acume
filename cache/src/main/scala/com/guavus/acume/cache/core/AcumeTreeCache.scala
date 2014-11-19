
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
import com.guavus.acume.cache.workflow.RequestType.RequestType
import com.guavus.acume.cache.workflow.RequestType.Timeseries

/**
 * @author archit.thakur
 *
 */
private [cache] class AcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache(acumeCacheContext, conf, cube) {

//  val cachePointToTable: MutableMap[LevelTimestamp, String] = MutableMap[LevelTimestamp, String]()
  val dimensionTable: DimensionTable = DimensionTable("AcumeCacheGlobalDimensionTable" + cube.cubeName)
  val diskUtility = DataLoader.getDataLoader(acumeCacheContext, conf, this)
//  val evictionpolicy = EvictionPolicy.getEvictionPolicy(cube.evictionPolicyClass, conf)
  
  override def createTempTable(startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, false)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, false)
    }
  }
		
  val cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
  .maximumSize(1000).removalListener(new RemovalListener[LevelTimestamp, String] {
	  def onRemoval(notification : RemovalNotification[LevelTimestamp, String]) {
	    acumeCacheContext.sqlContext.uncacheTable(notification.getValue())
	  }
  })
  .build(
      new CacheLoader[LevelTimestamp, String]() {
        def load(key: LevelTimestamp): String = {
          return getData(key);
        }
      });
  
  private def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf("_"))
  
  override def createTempTableAndMetadata(startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData = {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, true)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, true)
    }
  }
  
  private def createTableForAggregate(startTime: Long, endTime: Long, tableName: String, isMetaData: Boolean): MetaData = {
    // based on start time end time find the best possible path which depends on the level configured in variableretentionmap.
    val duration = endTime - startTime
    val levelTimestampMap = cacheLevelPolicy.getRequiredIntervals(startTime, endTime)
    buildTableForIntervals(levelTimestampMap, tableName, isMetaData)
  }
  
  private def createTableForTimeseries(startTime: Long, endTime: Long, tableName: String, queryOptionalParam: Option[QueryOptionalParam], isMetaData: Boolean): MetaData = {
    //based on timeseries policy use the appropriate level to create list of rdds needed to create the output table
    
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
    val intervals: MutableMap[Long, MutableList[Long]] = MutableMap(level -> list)
    buildTableForIntervals(intervals, tableName, isMetaData)
  }
  
  private def getVariableRetentionMap: SortedMap[Long, Int] = {
    val contextCollection = acumeCacheContext.vrmap
    SortedMap[Long, Int]() ++ contextCollection
  }
  
  private def getData(levelTimestamp: LevelTimestamp) = {

    import acumeCacheContext.sqlContext._
    val cacheLevel = levelTimestamp.level
    val diskread = diskUtility.loadData(cube, levelTimestamp, dimensionTable)
    val _tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    acumeCacheContext.sqlContext.applySchema(diskread, diskread.schema)
    diskread.registerTempTable(_tableName)
    cacheTable(_tableName) 
    _tableName
  }
  
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
        val tblNm = cachePointToTable.get(levelTimestamp)
        
//        val evictableCandidate = evictionpolicy.getEvictableCandidate(cachePointToTable.asMap.keySet.toList, cube)
//        if(evictableCandidate != null) {
//          Utility.evict(acumeCacheContext.sqlContext, evictableCandidate, cachePointToTable.get(evictableCandidate), cachePointToTable)
//        }
        
        val diskread = acumeCacheContext.sqlContext.sql(s"select * from $tblNm")
        finalSchema = diskread.schema
        val _$diskread = acumeCacheContext.sqlContext.applySchema(diskread, finalSchema)
        
        _$diskread
      }
      timeIterated
    }
    val schemarddlist = levelTime.flatten
    applySchema(schemarddlist.map(_.asInstanceOf[RDD[Row]]).reduce(_.union(_)), finalSchema).registerTempTable(tableName)
    val klist = timestamps.toList
    MetaData(klist)
  }
}