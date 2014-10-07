package com.guavus.acume.cache.core

import scala.collection.mutable.{ Map => MutableMap }
import java.util.SortedMap
import com.guavus.acume.cache.util.Utility
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.acume.cache.workflow.RequestType._
import com.guavus.acume.cache.common.CacheLevel._
import com.guavus.acume.cache.util.QueryOptionalParam
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.disk.utility.ORCDataLoader
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.gen.Acume
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.util.Utility12345
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.disk.utility.DataLoader

/**
 * Saves the dimension table till date and all fact tables as different tableNames for each levelTimestamp
 */
private [cache] class AcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache(acumeCacheContext, conf, cube) {

  val cachePointToTable: MutableMap[LevelTimestamp, String] = MutableMap[LevelTimestamp, String]()
  val dimensionTable: String = s"AcumeCacheGlobalDimensionTable${cube.cubeName}"
  def createTempTable(startTime : Long, tableName: String, endTime : Long, callType : RequestType.Value, queryOptionalParam: Option[QueryOptionalParam]) {
    callType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam)
    }
  }
  
  def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf(AcumeConstants.TRIPLE_DOLLAR_SSC) + 1)
  
  def createTableForAggregate(startTime: Long, endTime: Long, tableName: String) {
    // based on start time end time find the best possible path which depends on the level configured in variableretentionmap.
    val duration = endTime - startTime
    val levelTimestampMap = cacheLevelPolicy.getRequiredIntervals(startTime, endTime)
    buildTableForIntervals(levelTimestampMap, tableName)
  }
  
  def createTableForTimeseries(startTime: Long, endTime: Long, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    //based on timeseries policy use the appropriate level to create list of rdds needed to create the output table
    
    val baseLevel = cube.baseGran.getGranularity
    val level = 
      queryOptionalParam match{
      case Some(param) =>
        if(param.getTimeSeriesGranularity() != null) {
          var level = Math.max(baseLevel, param.getTimeSeriesGranularity());
			val variableRetentionMap = getVariableRetentionMap(conf.get(ConfConstants.variableretentionmap))
			if(!variableRetentionMap.containsKey(level)) {
				val headMap = variableRetentionMap.headMap(level);
				if(headMap.size() == 0) {
					throw new IllegalArgumentException("Wrong granularity " + level + " passed in request which is not present in variableRetentionMap ");
				}
				level = headMap.lastKey();
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
    val list = Utility12345.getAllIntervals(startTimeCeiling, endTimeFloor, level)
    val intervals: MutableMap[Long, MutableList[Long]] = MutableMap(level -> list)
    buildTableForIntervals(intervals, tableName)
  }
  
  private def getVariableRetentionMap(variableRetentionMap: String): SortedMap[Long, Int] = null 
  
  private def buildTableForIntervals(levelTimestampMap: MutableMap[Long, MutableList[Long]], tableName: String) {
    import acumeCacheContext.sqlContext._
    var flag = false
    val diskUtility = DataLoader.getDataLoader(acumeCacheContext, conf, cube)
    val businessCube = AcumeCacheContext.cubeMap.getOrElse(getCubeName(tableName), throw new RuntimeException("Cube " + tableName + " doesn't exist."))
    for(levelTsMapEntry <- levelTimestampMap){
      val (level, ts) = levelTsMapEntry
      val cachelevel = CacheLevel.getCacheLevel(level)
      for(item <- ts){
        val levelTimestamp = LevelTimestamp(cachelevel, item)
        val _$tableName = cube.toString + levelTimestamp.toString
        val diskread = 
          if(!cachePointToTable.contains(levelTimestamp)){
            diskUtility.loadData(businessCube, levelTimestamp, dimensionTable)
          } else{
            acumeCacheContext.sqlContext.sql("select * from " + cachePointToTable.get(LevelTimestamp(cachelevel, item)))
          }
        if(!flag){
          diskread.registerTempTable(tableName)
          flag = true
        }
        else{
          diskread.insertInto(tableName)
        }
        diskread.registerTempTable(_$tableName)
        cachePointToTable.put(levelTimestamp, _$tableName)
      }
    }
  }
}