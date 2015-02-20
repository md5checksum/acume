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
import java.util.Observable
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.common.LevelTimestamp


/**
 * @author archit.thakur
 *
 */
abstract class AcumeCache[k, v](val acumeCacheContext: AcumeCacheContext, val conf: AcumeCacheConf, val cube: Cube) extends Observable {

  protected val list = new MutableList[AcumeCacheObserver]
  
  var cachePointToTable : com.google.common.cache.LoadingCache[k , v] = _
  /**
   * This will take the call type and startTime , endTime and then generate the tempTable by joining dimension table and corresponding factTables. 
   * it might have to search for all the fact tables which will be used to calculate the data set.
   */
  def newObserverAddition(acumeCacheObserver: AcumeCacheObserver) = {
    
    list.+=(acumeCacheObserver)
  }
  
  def getDataFromBackend(levelTimestamp: LevelTimestamp) : v = {
    import acumeCacheContext.sqlContext._
    import scala.StringContext._
    val _tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    try {
    	val pointRdd = acumeCacheContext.sqlContext.table(_tableName)
    	print(s"Recalculating data for $levelTimestamp as it was evicted earlier")
    	pointRdd.cache
    	_getDataFromBackend(pointRdd, levelTimestamp)
    } catch {
      case _ : Exception => print(s"Getting data from Insta for $levelTimestamp as it was never calculated")
    }
    _getDataFromBackend(null, levelTimestamp)
  }
  
  def _getDataFromBackend(pointRdd : SchemaRDD , levelTimestamp: LevelTimestamp) : v
  
  def getCacheCollection =  cachePointToTable
  
  def createTempTable(keyMap : Map[String, Any], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam])
  
  def createTempTableAndMetadata(keyMap : Map[String, Any], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData 	

}