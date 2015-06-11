package com.guavus.acume.cache.core

import java.io.File
import java.util.concurrent.Executors

import scala.collection.JavaConversions._
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
import com.guavus.acume.cache.common.ConfConstants

abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCache])
  
  deleteExtraDataFromDiskAtStartUp
  
  def deleteExtraDataFromDiskAtStartUp() {
    println("Starting deleting file")
    try {
      val cacheDirectory = acumeCacheContext.cacheConf.get(ConfConstants.cacheBaseDirectory) + File.separator + acumeCacheContext.cacheSqlContext.sparkContext.getConf.get("spark.app.name") + "-" + acumeCacheContext.cacheConf.get(ConfConstants.cacheDirectory) + File.separator + cube.binsource + File.separator + cube.cubeName
      val path = new Path(cacheDirectory)
      val fs = path.getFileSystem(acumeCacheContext.cacheSqlContext.sparkContext.hadoopConfiguration)
      val directories = fs.listStatus(path).map(x => { CacheLevel.nameToLevelMap.get(x.getPath().getName()) }).map(x => { x.get.localId.longValue })
      val notPresentLevels = directories.filter(!cube.diskLevelPolicyMap.contains(_))
      print("Directorie are " + directories)
      print(notPresentLevels.mkString(","))
      for (notPresent <- notPresentLevels) {
        logger.info("deleting directory {} ",  (cacheDirectory + File.separator + CacheLevel.getCacheLevel(notPresent)))
        fs.delete(new Path(cacheDirectory + File.separator + CacheLevel.getCacheLevel(notPresent)))
      }
      val presentLevels = directories.filter(cube.diskLevelPolicyMap.contains(_))
      println(presentLevels.mkString(","))
      for (presentLevel <- presentLevels) {
        val timestamps = fs.listStatus(new Path(cacheDirectory + File.separator + CacheLevel.getCacheLevel(presentLevel))).map(_.getPath().getName().toLong)
        for (timestamp <- timestamps)
          if (Utility.getPriority(timestamp, presentLevel, presentLevel, cube.levelPolicyMap, acumeCacheContext.getLastBinPersistedTime(cube.binsource)) == 0) {
            logger.info("deleting file {} " , (AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeCacheContext, cube, new LevelTimestamp(CacheLevel.getCacheLevel(presentLevel), timestamp))))
            fs.delete(new Path(AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeCacheContext, cube, new LevelTimestamp(CacheLevel.getCacheLevel(presentLevel), timestamp))))
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
      var cacheValue : AcumeTreeCacheValue = null
      // Check if a combined point is available enclosing this timeRange
      val combinedPointsOfCache = cachePointToTable.asMap().filterKeys(x => (x.aggregationLevel != x.level) && (x.level == key.level)).toMap
      var tempTimeRange = Long.MaxValue
      for ((leveltimestamp, table) <- combinedPointsOfCache) {
        val rangeStartTime = leveltimestamp.timestamp
        val rangeEndTime = Utility.getPreviousTimeForGranularity(Utility.getNextTimeFromGranularity(leveltimestamp.timestamp, leveltimestamp.aggregationLevel.localId, Utility.newCalendar()), leveltimestamp.level.localId, Utility.newCalendar())
        if(key.timestamp <= rangeEndTime ||  key.timestamp >= rangeStartTime) {
          if(tempTimeRange > rangeEndTime - rangeStartTime) {
            //Fetch the data from the smallest combined point.
            cacheValue = table
            tempTimeRange = rangeEndTime - rangeStartTime
          }
        }
      }
      if(cacheValue == null) {
        // If the timeStamp not found in combined point. Find it in cache
        cacheValue = cachePointToTable.get(key)
      }
      cacheValue
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
    logger.info("Combining level {} to {}", childlevel, aggregationLevel)
    val aggregatedDataTimestamp = Utility.floorFromGranularity(childTimestamp, aggregationLevel)
    val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), aggregatedDataTimestamp, LoadType.DISK, CacheLevel.getCacheLevel(aggregationLevel))
    var combinePoint = tryGet(aggregatedTimestamp)
    val childrenData = scala.collection.mutable.ArrayBuffer[AcumeTreeCacheValue]()
    if (combinePoint == null) {
      var shouldCombine = true
      breakable {
        val children = cacheLevelPolicy.getCombinableIntervals(aggregatedDataTimestamp, aggregationLevel, childlevel)
        logger.info("Children are {}", children)
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
        	  logger.info("Finally Combining level {} to aggregationlevel " + aggregationLevel + " and levelTimeStamp {} ", childlevel, aggregatedTimestamp)
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
              childrenData.foreach(x => {
            	  logger.info("Evicting child point" + x.getAcumeValue().levelTimestamp.toString())
              })
            	childrenData.map(_.evictFromMemory)
            	//Put the combined point to cachePointToTable
            	cachevalue.map(x => cachePointToTable.put(aggregatedTimestamp, x))
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



