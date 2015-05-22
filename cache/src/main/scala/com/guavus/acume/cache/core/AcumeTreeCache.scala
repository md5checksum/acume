package com.guavus.acume.cache.core

import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.utility.Utility
import scala.util.control.Breaks._
import org.apache.spark.sql.SchemaRDD
import org.apache.hadoop.fs.Path

abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {

  def checkIfTableAlreadyExist(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    import scala.StringContext._
    val _tableName = cube.cubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    try {
      val diskDirectory = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeCacheContext, cube, levelTimestamp)
      val path = new Path(diskDirectory)
	  val fs = path.getFileSystem(acumeCacheContext.cacheSqlContext.sparkContext.hadoopConfiguration)
	  //Do previous run cleanup
	  if(fs.exists(path)) {
	    val rdd = acumeCacheContext.cacheSqlContext.parquetFileIndivisible(diskDirectory)
	    return new AcumeFlatSchemaCacheValue(new AcumeDiskValue(levelTimestamp, cube, rdd), acumeCacheContext)
	  }
    } catch {
      case _: Exception =>  
    }
    null
  }

  protected def populateParent(childlevel: Long, childTimestamp: Long) {
    val parentSiblingMap = cacheLevelPolicy.getParentSiblingMap(childlevel, childTimestamp)
    for ((parent, children) <- parentSiblingMap) {
      val parentTimestamp = Utility.floorFromGranularity(childTimestamp, parent)
      val parentPoint = checkIfTableAlreadyExist(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp))
      if (parentPoint == null) {
        var shouldPopulateParent = true
        breakable {
          for (child <- children) {
            val childData = checkIfTableAlreadyExist(new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child))
            if (childData == null) {
              shouldPopulateParent = false
              break
            }
          }
        }
        if (shouldPopulateParent) {
          val parentData = cachePointToTable.get(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp, false))
          notifyObserverList
          populateParent(parent, Utility.floorFromGranularity(childTimestamp, parent))
        }
      }
    }
  }
  
  override def evict(key : LevelTimestamp) {
    val value = cachePointToTable.get(key)
    if(value != null)
    	value.evictFromMemory
  }

  def mergePathRdds(rdds : Iterable[SchemaRDD]) = {
    	rdds.reduce(_.unionAll(_))
  }
  
}