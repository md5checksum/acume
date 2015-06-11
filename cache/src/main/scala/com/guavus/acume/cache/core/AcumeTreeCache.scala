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
import com.guavus.acume.cache.common.LoadType
import com.guavus.acume.cache.common.LevelTimestamp

abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {

  def checkIfTableAlreadyExist(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    import scala.StringContext._
    try {
      val diskDirectory = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeCacheContext, cube, levelTimestamp)
      val diskDirpath = new Path(diskDirectory)
	  //Do previous run cleanup
	  if(AcumeTreeCacheValue.isPathExisting(diskDirpath, acumeCacheContext) && AcumeTreeCacheValue.isDiskWriteComplete(diskDirectory, acumeCacheContext)) {
	    val rdd = acumeCacheContext.cacheSqlContext.parquetFileIndivisible(diskDirectory)
	    return new AcumeFlatSchemaCacheValue(new AcumeDiskValue(levelTimestamp, cube, rdd), acumeCacheContext)
	  }
    } catch {
      case _: Exception =>  
    }
    null
  }
  
  def get(key: LevelTimestamp) = {
    val cacheValue = cachePointToTable.get(key)
    AcumeCacheContextTrait.addAcumeTreeCacheValue(cacheValue)
    cacheValue
  }
  
  def tryGet(key : LevelTimestamp) = {
    try {
        key.loadType = LoadType.DISK
    	cachePointToTable.get(key)
    } catch {
      case e : java.util.concurrent.ExecutionException => if(e.getCause().isInstanceOf[NoDataException]) null else throw e
    }
  }

  protected def populateParent(childlevel: Long, childTimestamp: Long) {
    val parentSiblingMap = cacheLevelPolicy.getParentSiblingMap(childlevel, childTimestamp)
    for ((parent, children) <- parentSiblingMap) {
      val parentTimestamp = Utility.floorFromGranularity(childTimestamp, parent)
      val parentPoint = tryGet(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp, LoadType.DISK))
      if (parentPoint == null) {
        var shouldPopulateParent = true
        breakable {
          for (child <- children) {
            val childData = tryGet(new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child, LoadType.DISK))
            if (childData == null) {
              shouldPopulateParent = false
              break
            }
          }
        }
        if (shouldPopulateParent) {
          val parentData = cachePointToTable.get(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp, LoadType.InMemory))
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
