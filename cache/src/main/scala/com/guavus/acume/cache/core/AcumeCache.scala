package com.guavus.acume.cache.core

abstract class AcumeCache {

  /**
   * This will take the call type and startTIme , endTime and then generate the tempTable by joining dimension table and corresponding factTables. 
   * it might have to search for all the fact tables which will be used to calculate the data set.
   */
//  def createTempTable(startTime : Long, endTime : Long, requestType : RequestType.Value)
}