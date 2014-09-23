package com.guavus.acume.cache

import scala.collection.mutable.{ Map => MutableMap }
import com.guavus.acume.core.LevelTimestamp
import com.guavus.acume.workflow.RequestType
import com.guavus.acume.workflow.RequestType._

/**
 * Saves the dimension table till date and all fact tables as different tableNames for each levelTimestamp
 */
class AcumeTreeCache(name: String , cachePointToTable: MutableMap[LevelTimestamp, String], dimensionTable: String, variableRetentionMap: String, timeSeriesPolicyMap: String) extends AcumeCache(name) {

  def createTempTable(startTime : Long, endTime : Long, callType : RequestType.Value) = {
    callType match {
      case Aggregate => 
      case Timeseries => 
    }
  }
  
  def createTableForAggregate() {
    // based on start time end time find the best possible path which depends on the level configured in variableretentionmap.
  }
  
  def createTableForTimeseries() {
    //based on timeseries policy use the appropriate level to create list of rdds needed to create the output table 
  }
  
}