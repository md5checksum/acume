package com.guavus.acume.cache.core

import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf

abstract class AcumeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) {

  /**
   * This will take the call type and startTIme , endTime and then generate the tempTable by joining dimension table and corresponding factTables. 
   * it might have to search for all the fact tables which will be used to calculate the data set.
   */
//  def createTempTable(startTime : Long, endTime : Long, requestType : RequestType.Value)
  
}