
package com.guavus.acume.cache.core

import scala.collection.immutable.SortedMap
import scala.collection.mutable.{ Map => MutableMap }
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.acume.cache.workflow.RequestType._
import com.guavus.acume.cache.common.CacheLevel._
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.disk.utility.ORCDataLoader
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.gen.Acume
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.utility.Utility
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.disk.utility.DataLoader
import org.apache.spark.sql.SchemaRDD
import com.google.common.cache.CacheBuilder
import com.guavus.acume.cache.common.LevelTimestamp
import com.google.common.cache.CacheLoader
import com.guavus.acume.cache.workflow.MetaData

/**
 * Saves the dimension table till date and all fact tables as different tableNames for each levelTimestamp
 */
private [cache] class AcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache(acumeCacheContext, conf, cube) {

//  val cachePointToTable: MutableMap[LevelTimestamp, String] = MutableMap[LevelTimestamp, String]()
  val dimensionTable: String = s"AcumeCacheGlobalDimensionTable${cube.cubeName}"
  val diskUtility = DataLoader.getDataLoader(acumeCacheContext, conf, cube)
  
  override def createTempTable(startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, false)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, false)
    }
  }
		
  val cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
  .maximumSize(conf.get(ConfConstants.rrsize._1).toInt)
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
        Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime))
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
    val _$tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    diskread.registerTempTable(_$tableName)
    cacheTable(_$tableName) 
    _$tableName
  }
  
  private def buildTableForIntervals(levelTimestampMap: MutableMap[Long, MutableList[Long]], tableName: String, isMetaData: Boolean): MetaData = {
    import acumeCacheContext.sqlContext._
    val timestamps: MutableList[Long] = MutableList[Long]()
    var flag = false
    val x = getCubeName(tableName)
    for(levelTsMapEntry <- levelTimestampMap){
      val (level, ts) = levelTsMapEntry
      val cachelevel = CacheLevel.getCacheLevel(level)
      for(item <- ts){
        timestamps.+=(item)
        val levelTimestamp = LevelTimestamp(cachelevel, item)
//        val _$tableName = cube.toString + levelTimestamp.toString
//        val diskread = 
//          if(!cachePointToTable.contains(levelTimestamp)){
//            (diskUtility.loadData(businessCube, levelTimestamp, dimensionTable), true)
//          } else{
//            (acumeCacheContext.sqlContext.sql("select * from " + cachePointToTable.get(LevelTimestamp(cachelevel, item))), false)
//          }
        
        val tblNm = cachePointToTable.get(levelTimestamp)
        val diskread = acumeCacheContext.sqlContext.sql(s"select * from $tblNm")
        if(!flag){
          diskread.registerTempTable(tableName)
          flag = true
        }
        else{
          diskread.insertInto(tableName)
        }
//        diskread._1.registerTempTable(_$tableName)
//        if(diskread._2)
//          cacheTable(_$tableName) 
//        cachePointToTable.put(levelTimestamp, _$tableName)
      }
    }
    val klist = timestamps.toList
    MetaData(klist)
  }
}