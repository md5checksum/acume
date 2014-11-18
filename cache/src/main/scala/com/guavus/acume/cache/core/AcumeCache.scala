package com.guavus.acume.cache.core

import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.acume.cache.workflow.RequestType._
import com.guavus.acume.cache.utility.QueryOptionalParam
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.workflow.AcumeCacheResponse
import com.guavus.acume.cache.workflow.MetaData

/**
 * @author archit.thakur
 *
 */
abstract class AcumeCache(val acumeCacheContext: AcumeCacheContext, val conf: AcumeCacheConf, val cube: Cube) {

  /**
   * This will take the call type and startTIme , endTime and then generate the tempTable by joining dimension table and corresponding factTables. 
   * it might have to search for all the fact tables which will be used to calculate the data set.
   */
  def createTempTable(startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam])
  def createTempTableAndMetadata(startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData 	

}