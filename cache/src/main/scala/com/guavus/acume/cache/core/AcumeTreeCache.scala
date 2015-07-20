package com.guavus.acume.cache.core

import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.utility.Utility
import scala.util.control.Breaks._
import org.apache.spark.sql.SchemaRDD
import org.apache.hadoop.fs.Path
import com.guavus.acume.cache.common.LoadType
import com.guavus.acume.cache.common.ConfConstants
import java.io.File
import com.guavus.acume.cache.common.LevelTimestamp
import org.slf4j.LoggerFactory
import org.slf4j.Logger

abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCache])
  
  deleteExtraDataFromDiskAtStartUp
  
  def deleteExtraDataFromDiskAtStartUp() {
    logger.info("Starting deleting file")
    try {
      val cacheDirectory = acumeCacheContext.cacheConf.get(ConfConstants.cacheBaseDirectory) + File.separator + acumeCacheContext.cacheSqlContext.sparkContext.getConf.get("spark.app.name") + "-" + acumeCacheContext.cacheConf.get(ConfConstants.cacheDirectory) + File.separator + cube.binsource + File.separator + cube.cubeName
      val path = new Path(cacheDirectory)
      val fs = path.getFileSystem(acumeCacheContext.cacheSqlContext.sparkContext.hadoopConfiguration)
      val directories = fs.listStatus(path).map(x => { CacheLevel.nameToLevelMap.get(x.getPath().getName()) }).map(x => { x.get.localId.longValue })
      val notPresentLevels = directories.filter(!cube.diskLevelPolicyMap.contains(_))
      print("Directorie are " + directories)
      print(notPresentLevels.mkString(","))
      for (notPresent <- notPresentLevels) {
        logger.info("deleting directory {} ",  (cacheDirectory + File.separator +CacheLevel.getCacheLevel(notPresent)))
        AcumeTreeCacheValue.deleteDirectory(cacheDirectory + File.separator + CacheLevel.getCacheLevel(notPresent), acumeCacheContext)
      }
      val presentLevels = directories.filter(cube.diskLevelPolicyMap.contains(_))
      logger.info(presentLevels.mkString(","))
      for (presentLevel <- presentLevels) {
        val timestamps = fs.listStatus(new Path(cacheDirectory + File.separator + CacheLevel.getCacheLevel(presentLevel))).map(_.getPath().getName().toLong)
        for (timestamp <- timestamps)
          if (Utility.getPriority(timestamp, presentLevel, cube.levelPolicyMap, acumeCacheContext.getLastBinPersistedTime(cube.binsource)) == 0) {
            val dir = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeCacheContext, cube, new LevelTimestamp(CacheLevel.getCacheLevel(presentLevel), timestamp))
            logger.info("deleting file {} " , dir)
            AcumeTreeCacheValue.deleteDirectory(dir, acumeCacheContext)
          }
      }
    } catch {
      case e: Exception => logger.warn(e.getMessage())
    }
  }

  def checkIfTableAlreadyExist(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    import scala.StringContext._
    try {
      val diskDirectory = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeCacheContext, cube, levelTimestamp)
      val diskDirpath = new Path(diskDirectory)
  	  //Do previous run cleanup
  	  if(AcumeTreeCacheValue.isPathExisting(diskDirpath, acumeCacheContext) && AcumeTreeCacheValue.isDiskWriteComplete(diskDirectory, acumeCacheContext)) {
  	    acumeCacheContext.cacheSqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk cache reading " + diskDirectory, false)
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
    Utility.withDummyCallSite(acumeCacheContext.cacheSqlContext.sparkContext) {
      rdds.reduce(_.unionAll(_))
    }
  }
  
}
