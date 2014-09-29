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
import com.guavus.acume.cache.disk.utility.DiskUtility
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.gen.Acume
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.util.Utility12345
import scala.collection.mutable.MutableList

/**
 * Saves the dimension table till date and all fact tables as different tableNames for each levelTimestamp
 */
class AcumeTreeCache(name: String, baseLevel: Long, acumeCube: Acume, sqlContext: SQLContext, conf: AcumeCacheConf, variableRetentionMap: String, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) extends AcumeCache {

  val cachePointToTable: MutableMap[LevelTimestamp, String] = MutableMap[LevelTimestamp, String]()
  val dimensionTable: String = name + "CacheGlobalDimensionTable"
//  val evictionDetails = 
  var cacheLevelPolicy: CacheLevelPolicyTrait = null
  def createTempTable(startTime : Long, tableName: String, endTime : Long, callType : RequestType.Value) {
    callType match {
      case Aggregate => 
      case Timeseries => 
    }
  }
  
  def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf(AcumeConstants.TRIPLE_DOLLAR_SSC) + 1)
  
  def createTableForAggregate(startTime: Long, endTime: Long, tableName: String) {
    // based on start time end time find the best possible path which depends on the level configured in variableretentionmap.
    val diskUtility = new DiskUtility(sqlContext, null, null, acumeCube)
    val levels = Array[Long](300, 3600, 86400, 2592000, 5184000, 10368000)
    cacheLevelPolicy = new FixedLevelPolicy(levels, baseLevel)
    val duration = endTime - startTime
    val mx = cacheLevelPolicy.getRequiredIntervals(startTime, endTime)
    import sqlContext._
    var flag = false
    for(mapEntry <- mx){
      val (level, ts) = mapEntry
      for(item <- ts){
        val cachelevel = CacheLevel.getCacheLevel(level)
        if(!cachePointToTable.contains(LevelTimestamp(cachelevel, item))){
          if(!flag){
            diskUtility.getRDDLineage(getCubeName(tableName), cachelevel, item, dimensionTable).registerTempTable(tableName)
            flag = true
          } 
          else
            diskUtility.getRDDLineage(getCubeName(tableName), cachelevel, item, dimensionTable).insertInto(tableName)
        }
      }
    }
  }
  
  def createTableForTimeseries(startTime: Long, endTime: Long, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    //based on timeseries policy use the appropriate level to create list of rdds needed to create the output table
    
    val level = 
      queryOptionalParam match{
      case Some(param) =>
        if(param.getAggregationPolicyPeak()!=null || param.getUseBaseGran())
          baseLevel
        else if(param.getTimeSeriesGranularity() != null && param.getTimeSeriesGranularity() != 0) {
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
    intervals
  }
  
  private def getVariableRetentionMap(variableRetentionMap: String): SortedMap[Long, Int] = null 
  
}