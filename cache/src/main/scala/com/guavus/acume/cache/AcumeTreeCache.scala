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
  val cahceLevelPolicy: CacheLevelPolicyTrait = null
  def createTempTable(startTime : Long, endTime : Long, callType : RequestType.Value) = {
    callType match {
      case Aggregate => 
      case Timeseries => 
    }
  }
  
  def createTableForAggregate(startTime: Long, endTine: Long, requestDataType: RequestType.Value, queryOptionalParams: QueryOptionalParam) {
    // based on start time end time find the best possible path which depends on the level configured in variableretentionmap.
    
    
    val variableRetentionMap = AcumeConfiguration.VariableRetentionMap.getValue()
		val levels = new Long[variableRetentionMap.size()];
		List<Long> allLevels = new ArrayList<Long>();
		int i = 0;
		boolean containsBaseLevel = false;
		for(Entry<Long, Integer>entry:variableRetentionMap.entrySet()){
			if(entry.getKey() < timeGranularity.getGranularity()){
				logger.error("Error in retention map for cube {} . " +
						"Level specified in retention map is smaller than base level",
						cacheIdentifier);
				throw new IllegalArgumentException("Error in retention map for cube "+cacheIdentifier+" . " +
						"Level specified in retention map is smaller than base level");
			}
			if(entry.getKey() == timeGranularity.getGranularity()){
				containsBaseLevel = true;
			}
			levels[i] = entry.getKey();
			allLevels.add(entry.getKey());
			i++;
		}
		cacheLevelPolicy = new FixedLevelPolicy(levels, timeGranularity.getGranularity());
	
    val requiredIntervals = shortestPath(startTime, endTime);
  }
  
  def createTableForTimeseries() {
    //based on timeseries policy use the appropriate level to create list of rdds needed to create the output table 
  }
  
}