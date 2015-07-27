package com.guavus.acume.cache.core

import scala.Array.canBuildFrom
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.types.StructField
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.MetaData
import com.guavus.acume.cache.workflow.RequestType.Aggregate
import com.guavus.acume.cache.workflow.RequestType.RequestType
import com.guavus.acume.cache.workflow.RequestType.Timeseries
import com.guavus.acume.cache.common.LevelTimestamp
import com.google.common.cache.CacheBuilder
import com.guavus.acume.cache.common.ConfConstants
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import com.google.common.cache.CacheLoader
import scala.collection.mutable.LinkedList
import scala.collection.mutable.HashMap

class SingleEntityAcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]](acumeCacheContext, conf, cube) {
  val cacheSize = conf.getInt(ConfConstants.acumeCacheSingleEntityCacheSize).getOrElse(throw new IllegalArgumentException("Property not set  : " + ConfConstants.acumeCacheSingleEntityCacheSize))
  cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.getInt(ConfConstants.rrcacheconcurrenylevel).get)
  .maximumSize(cacheSize).removalListener(new RemovalListener[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]] {
	  def onRemoval(notification : RemovalNotification[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]]) {
//	    clear all tables of the TreCache. for this create a method in tree cache which can remove all data from it.
//	    acumeCacheContext.sqlContext.uncacheTable(notification.getValue().measuretableName)
	  }
  })
  .build(
      new CacheLoader[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]]() {
        def load(key: CacheIdentifier): AcumeCache[LevelTimestamp, AcumeTreeCacheValue] = {
          return getDataFromBackend(key);
        }
      });
  
  override def getDataFromBackend(cacheIdentifier : CacheIdentifier) : AcumeCache[LevelTimestamp, AcumeTreeCacheValue] = {
	AcumeCacheFactory.getInstance(acumeCacheContext, conf, cacheIdentifier, cube)
  }
  
  override def evict(key : CacheIdentifier) {
    //TODO Remove all points related to the cache which was removed
  }

  def createTempTable(keyMap : List[Map[String, Any]], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) = {
	  val tempTables = for(keyValueMap <- keyMap) yield {
	    val cacheIdentifier = new CacheIdentifier()
	    cacheIdentifier.put("cube", cube.hashCode)
	    for((key, value) <- keyValueMap) {
	    	cacheIdentifier.put(key, value)
	    }
	    val singleEntityCache = cachePointToTable.get(cacheIdentifier)
	    val tableNameAppender = keyValueMap.map(x => x._1+ "=" + x._2).mkString("_")
	    singleEntityCache.createTempTable(List(keyValueMap.toMap), startTime, endTime, requestType, (tableName + "_" + tableNameAppender), queryOptionalParam)
	    acumeCacheContext.sqlContext.table((tableName + "_" + tableNameAppender))
	  }
	  val finalRdd = tempTables.reduce(_.unionAll(_))
	  finalRdd.registerTempTable(tableName)
  }
  
  def createTempTableAndMetadata(keyMap : List[Map[String, Any]], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData = {
    val tempTables = for(keyValueMap <- keyMap) yield {
	    val cacheIdentifier = new CacheIdentifier()
	    cacheIdentifier.put("cube", cube.hashCode)
	    for((key, value) <- keyValueMap) {
	    	cacheIdentifier.put(key, value)
	    }
	    val singleEntityCache = cachePointToTable.get(cacheIdentifier)
	    val tableNameAppender = keyValueMap.map(x => x._1+ "=" + x._2).mkString("_")
	    val metadata = singleEntityCache.createTempTableAndMetadata(keyMap, startTime, endTime, requestType, (tableName + "_" + tableNameAppender), queryOptionalParam)
	    (acumeCacheContext.sqlContext.table((tableName + "_" + tableNameAppender)),metadata)
	  }
	  val finalRdd = tempTables.reduce((x,y)=>(x._1.unionAll(y._1), x._2))
	  finalRdd._1.registerTempTable(tableName)
	  finalRdd._2
  } 	


}