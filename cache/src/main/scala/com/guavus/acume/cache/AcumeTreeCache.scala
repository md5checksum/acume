package com.guavus.acume.cache

import scala.collection.mutable.{ Map => MutableMap }
import com.guavus.acume.core.LevelTimestamp
import com.guavus.acume.workflow.RequestType
import com.guavus.acume.workflow.RequestType._
import com.guavus.acume.util.QueryOptionalParam
import com.guavus.acume.configuration.AcumeConfiguration

/**
 * Saves the dimension table till date and all fact tables as different tableNames for each levelTimestamp
 */
class AcumeTreeCache(name: String , cachePointToTable: MutableMap[LevelTimestamp, String], dimensionTable: String, variableRetentionMap: String, timeSeriesPolicyMap: String) extends AcumeCache(name) {

//  val evictionDetails = 
  var cacheLevelPolicy: CacheLevelPolicyTrait = null
//  def createTempTable(startTime : Long, tableName: String, endTime : Long, callType : RequestType.Value) {
//    callType match {
//      case Aggregate => 
//      case Timeseries => 
//    }
//  }
//  
//  def createTableForAggregate(startTime: Long, endTime: Long) {
//    // based on start time end time find the best possible path which depends on the level configured in variableretentionmap.
//    val levels = Array[Long](300, 3600, 86400, 2592000)
//    cacheLevelPolicy = new FixedLevelPolicy(levels, 300)
//    val duration = endTime - startTime
//    cacheLevelPolicy.getRequiredIntervals(startTime, endTime)
//  }
//  
//  def createTableForTimeseries() {
//    //based on timeseries policy use the appropriate level to create list of rdds needed to create the output table 
//  }
  
}