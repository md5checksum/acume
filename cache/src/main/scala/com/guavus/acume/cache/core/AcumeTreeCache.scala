package com.guavus.acume.cache.core

import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.rubix.cache.util.CacheUtils
import com.guavus.acume.cache.utility.Utility
import scala.util.control.Breaks._

private[cache] abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {

  def checkIfTableAlreadyExist(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
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

  protected def populateParent(childlevel: Long, childTimestamp: Long) {
    val parentSiblingMap = cacheLevelPolicy.getParentSiblingMap(childlevel, childTimestamp)
    for ((parent, children) <- parentSiblingMap) {
      val parentTimestamp = Utility.floorFromGranularity(childTimestamp, parent)
      val parentPoint = cachePointToTable.getIfPresent(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp))
      if (parentPoint == null) {
        var shouldPopulateParent = true
        breakable {
          for (child <- children) {
            val childData = cachePointToTable.getIfPresent(new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child))
            if (childData == null) {
              shouldPopulateParent = false
              break
            } else {
              CacheUtils.getCachedElement.put(new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child), childData)
            }
          }
        }
        if (shouldPopulateParent) {
          val parentData = cachePointToTable.get(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp, false))
          CacheUtils.getCachedElement.put(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp), parentData)
          notifyObserverList
          populateParent(parent, Utility.floorFromGranularity(childTimestamp, parent))
        }

      }
    }
  }

}