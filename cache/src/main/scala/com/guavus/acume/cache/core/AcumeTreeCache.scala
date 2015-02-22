package com.guavus.acume.cache.core

import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp

private [cache] abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) 
extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {
  
	override def getDataFromBackend(levelTimestamp : LevelTimestamp) : AcumeTreeCacheValue = {
    import acumeCacheContext.sqlContext._
    import scala.StringContext._
    val _tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    try {
      val pointRdd = acumeCacheContext.sqlContext.table(_tableName)
      println(s"Recalculating data for $levelTimestamp as it was evicted earlier")
      pointRdd.cache
      return new AcumeTreeCacheValue(null, _tableName, pointRdd)
    } catch {
      case _: Exception => println(s"Getting data from Insta for $levelTimestamp as it was never calculated")
    }
    null
  }

}