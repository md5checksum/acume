package com.guavus.acume.cache.core

import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
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
import java.util.LinkedList
import scala.collection.mutable.HashMap

/**
 * @author archit.thakur
 *
 */
abstract class AcumeCache[k, v](val acumeCacheContext: AcumeCacheContextTrait, val conf: AcumeCacheConf, val cube: Cube) extends Observable {

  protected val list = new MutableList[AcumeCacheObserver]
  
  var cachePointToTable : com.google.common.cache.LoadingCache[k , v] = _
  /**
   * This will take the call type and startTime , endTime and then generate the tempTable by joining dimension table and corresponding factTables. 
   * it might have to search for all the fact tables which will be used to calculate the data set.
   */
  def newObserverAddition(acumeCacheObserver: AcumeCacheObserver) = {
    
    list.+=(acumeCacheObserver)
  }
  
  def getDataFromBackend(levelTimestamp: k) : v
  
  def getCacheCollection =  cachePointToTable
  
  def createTempTable(keyMap : List[Map[String, Any]], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam])
  
  def createTempTableAndMetadata(keyMap : List[Map[String, Any]], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData

  def getCachePoints(
      startTime: Long,
      endTime: Long,
      gran: Long,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) =
    throw new NotImplementedError("AcumeCache does not implement getCachePoints().")

  def getAggregateCachePoints(
      startTime: Long,
      endTime: Long,
      gran: Long,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) =
    throw new NotImplementedError("AcumeCache does not implement getCachePoints().")

  def notifyObserverList = {
    list.foreach(_.update(this, conf))
  }
  
  def evict(key : k)

}
