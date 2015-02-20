package com.guavus.acume.cache.core

import scala.Array.canBuildFrom
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.StructField
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

class SingleEntityAcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]](acumeCacheContext, conf, cube) {
  val cacheSize = conf.getInt(ConfConstants.acumeCacheSingleEntityCacheSize, throw new IllegalArgumentException("Property not set  : " + ConfConstants.acumeCacheSingleEntityCacheSize))
  cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
  .maximumSize(cacheSize).removalListener(new RemovalListener[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]] {
	  def onRemoval(notification : RemovalNotification[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]]) {
//	    clear all tables of the TreCache. for this create a method in tree cache which can remove all data from it.
//	    acumeCacheContext.sqlContext.uncacheTable(notification.getValue().measuretableName)
	  }
  })
  .build(
      new CacheLoader[CacheIdentifier, AcumeCache[LevelTimestamp, AcumeTreeCacheValue]]() {
        def load(key: CacheIdentifier): AcumeCache[LevelTimestamp, AcumeTreeCacheValue] = {
          return getData(key);
        }
      });
  
  def getData(cacheIdentifier : CacheIdentifier) : AcumeCache[LevelTimestamp, AcumeTreeCacheValue] = {
	AcumeCacheFactory.getInstance(acumeCacheContext, conf, cacheIdentifier, cube)
  }

  def createTempTable(keyMap : Map[String, Any], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) = {
	  val tempTables = for((key,value) <- keyMap) yield {
	    val cacheIdentifier = new CacheIdentifier()
	    cacheIdentifier.put("cube", cube.hashCode)
	    cacheIdentifier.put(key, value)
	    val singleEntityCache = cachePointToTable.get(cacheIdentifier)
	    singleEntityCache.createTempTable(keyMap, startTime, endTime, requestType, (tableName + "_" + key + "_" + value), queryOptionalParam)
	    acumeCacheContext.sqlContext.table((tableName + "_" + key + "_" + value))
	  }
	  val finalRdd = tempTables.reduce(_.unionAll(_))
	  finalRdd.registerTempTable(tableName)
  }
  
  def createTempTableAndMetadata(keyMap : Map[String, Any], startTime : Long, endTime : Long, requestType : RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData = {
    val tempTables = for((key,value) <- keyMap) yield {
	    val cacheIdentifier = new CacheIdentifier()
	    cacheIdentifier.put("cube", cube.hashCode)
	    cacheIdentifier.put(key, value)
	    val singleEntityCache = cachePointToTable.get(cacheIdentifier)
	    val metadata = singleEntityCache.createTempTableAndMetadata(keyMap, startTime, endTime, requestType, (tableName + "_" + key + "_" + value), queryOptionalParam)
	    (acumeCacheContext.sqlContext.table((tableName + "_" + key + "_" + value)),metadata)
	  }
	  val finalRdd = tempTables.reduce((x,y)=>(x._1.unionAll(y._1), x._2))
	  finalRdd._1.registerTempTable(tableName)
	  finalRdd._2
  } 	


}