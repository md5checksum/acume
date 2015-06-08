package com.guavus.acume.cache.core

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SchemaRDD
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.LoadType
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCache])
  
  def checkIfTableAlreadyExist(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    import scala.StringContext._
    try {
      val diskDirectory = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeCacheContext, cube, levelTimestamp)
    	val diskDirpath = new Path(diskDirectory)
      //Do previous run cleanup
      if (AcumeTreeCacheValue.isPathExisting(diskDirpath, acumeCacheContext) && AcumeTreeCacheValue.isDiskWriteComplete(diskDirectory, acumeCacheContext)) {
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

  def mergeChildPoints(rdds: Seq[SchemaRDD]): SchemaRDD = rdds.reduce(_.unionAll(_))

  protected def combineLevels(childlevel: Long, childTimestamp: Long) {
    //TODO check if this level has to be combined at any other level
    val aggregationLevel = cacheLevelPolicy.getAggregationLevel(childlevel)
    if(aggregationLevel == childlevel) {
      return
    }
    logger.info("[Kashish] Combining level {} to {}", childlevel, aggregationLevel)
    val aggregatedDataTimestamp = Utility.floorFromGranularity(childTimestamp, aggregationLevel)
    val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), aggregatedDataTimestamp, LoadType.DISK, CacheLevel.getCacheLevel(aggregationLevel))
    var combinePoint = tryGet(aggregatedTimestamp)
    val childrenData = scala.collection.mutable.ArrayBuffer[AcumeTreeCacheValue]()
    if (combinePoint == null) {
      var shouldCombine = true
      breakable {
        val children = cacheLevelPolicy.getCombinableIntervals(aggregatedDataTimestamp, aggregationLevel, childlevel)
        logger.info("[Kashish] Children are {}", children)
        for (child <- children) {
          val childLevelTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child, LoadType.DISK)
          val childData = tryGet(childLevelTimestamp)
          childrenData += childData
          if (childData == null) {
            shouldCombine = false
            logger.info("Not combining, child data is not present {}", childLevelTimestamp)
            break
          }
        }
      }
      if (shouldCombine) {
        var isSuccessCombiningPoint = true
        val context = AcumeTreeCache.context
        val f: Future[Option[AcumeFlatSchemaCacheValue]] = Future({
          if (tryGet(aggregatedTimestamp) == null) {
        	  logger.info("[Kashish] Finally Combining level {} to aggregationlevel " + aggregationLevel + " and levelTimeStamp {} ", childlevel, aggregatedTimestamp)
            Some(new AcumeFlatSchemaCacheValue(new AcumeInMemoryValue(aggregatedTimestamp, cube, mergeChildPoints(childrenData.map(_.getAcumeValue.measureSchemaRdd))), acumeCacheContext))
          } else {
            logger.info("Already present {}", aggregatedTimestamp)
            None
          }
        })(context)

        f.onComplete {
          case Success(cachevalue) =>
            if(cachevalue != None) {
            	//Combining successful. Evict the child points from memory 
            	logger.info("[Kashish] Evicting child points {} of combined point from memory", childrenData)
            	childrenData.map(_.evictFromMemory)
            	//Put the combined point to cachePointToTable
            	cachevalue.map(x => cachePointToTable.put(aggregatedTimestamp, x))
            	logger.info("[Kashish] Combined and added to cache {}", aggregatedTimestamp)              
            }
          case Failure(t) => isSuccessCombiningPoint = false
          logger.error("", t)
        }(context)
      }
    }
  }

  override def evict(key: LevelTimestamp) {
    val value = cachePointToTable.get(key)
    if (value != null)
      value.evictFromMemory
  }

  def mergePathRdds(rdds : Iterable[SchemaRDD]) = {
    Utility.withDummyCallSite(acumeCacheContext.cacheSqlContext.sparkContext) {
      rdds.reduce(_.unionAll(_))
    }
  }

}

object AcumeTreeCache {
  val executorService = Executors.newFixedThreadPool(1)
  val context = ExecutionContext.fromExecutorService(AcumeTreeCache.executorService)

}



