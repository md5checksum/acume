package com.guavus.acume.cache.core

import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions.asScalaSet
import scala.collection.JavaConversions.mapAsJavaMap
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.control.Breaks._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Row
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.LoadType
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil
import com.guavus.acume.threads.NamedThreadPoolFactory


abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCache].getName() + "-" + cube.getAbsoluteCubeName)
  
  deleteExtraDataFromDiskAtStartUp
  
  def deleteExtraDataFromDiskAtStartUp() {
    logger.info("Starting deleting file")
    
    try {
      val cacheBaseDirectory = Utility.getCubeBaseDirectory(acumeCacheContext, cube)
      
      // Get all the levels persisted in diskCache
      val directoryLevelValues = Utility.listStatus(acumeCacheContext, cacheBaseDirectory).map(directory => {
    	  val splitPath = directory.getPath().getName().split("-")
    	  if(splitPath.size ==1)
    		  (CacheLevel.nameToLevelMap.get(splitPath(0)).getOrElse(null),CacheLevel.nameToLevelMap.get(splitPath(0)).getOrElse(null))
    	  else
    		  (CacheLevel.nameToLevelMap.get(splitPath(0)).getOrElse(null),CacheLevel.nameToLevelMap.get(splitPath(1)).getOrElse(null))
        })
      
      logger.info("Levels on disk are {}", directoryLevelValues.map(level => level._1 + "-" + level._2).mkString(", "))
        
      // FilterOut combinedDirectories from diskCache
      val onlyRolledUpLevelValues = directoryLevelValues.filter(levelTuple => levelTuple._1 != levelTuple._2)
      
      // FilterOut only combineEnabled levels from diskPolicyMap
      val onlyRollingEnabledLevels = cube.diskLevelPolicyMap.entrySet.filter( level => level.getKey.level != level.getKey.aggregationLevel)
      
      //Find all the rolled-up directories which should not be present in diskCache.  
      val notPresentRolledUpLevels = onlyRolledUpLevelValues.filter(levelTuple => {
        onlyRollingEnabledLevels.filter( level => { 
          level.getKey().level == levelTuple._1.localId && level.getKey().aggregationLevel == levelTuple._2.localId
        }).size == 0
      })
      logger.info("Not present rolled-up levels are {} ", notPresentRolledUpLevels.map(x=> x._1 + "-" + x._2).mkString(","))
      
      /*
       * Delete all the rolled-up directories which are not present in diskLevelPolicyMap
       * Note: Not deleting the non rolled-up directories/timestamps. They will be removed according to eviction logic.
       * We could use the eviction logic to evict the rolledup points as well but this is to make deletion faster.
       */
      logger.info("Deleting the redundant rolled-up points from diskCache")
      for (notPresent <- notPresentRolledUpLevels) {
        val basedir = Utility.getCubeBaseDirectory(acumeCacheContext, cube) 
        val directoryToBeDeleted = basedir + File.separator + Utility.getlevelDirectoryName(notPresent._1, notPresent._2) 
        Utility.deleteDirectory(directoryToBeDeleted, acumeCacheContext)
      }

      // Evict the evictable timestamps of the levels persisted in diskCache
      logger.info("Deleting points based on eviction logic")
      for (presentLevel <- directoryLevelValues) {
        val levelDirectoryName = Utility.getlevelDirectoryName(presentLevel._1, presentLevel._2)
        val directoryName = cacheBaseDirectory + File.separator + levelDirectoryName
        val timestamps = Utility.listStatus(acumeCacheContext, directoryName).map(_.getPath().getName().toLong)
        for (timestamp <- timestamps) {
          if (Utility.getPriority(timestamp, presentLevel._1.localId, presentLevel._2.localId, cube.diskLevelPolicyMap, BinAvailabilityPoller.getLastBinPersistedTime(cube.binSource)) == 0) {
            val directoryToBeDeleted = Utility.getDiskDirectoryForPoint(acumeCacheContext, cube, new LevelTimestamp(presentLevel._1, timestamp, presentLevel._2))
            Utility.deleteDirectory(directoryToBeDeleted, acumeCacheContext)
          }
        }
      }
      
      // Cleanup. Find all such directories which are empty (doesnot contain any timestamp). Delete the base directory folder
      logger.info("Cleaning up empty directories")
      val cleanUpDirectoryNames = Utility.listStatus(acumeCacheContext, cacheBaseDirectory).map(directory => {
        val directoryTobeCheckedForDelete = cacheBaseDirectory + File.separator + directory
        val numOftimestamps = Utility.listStatus(acumeCacheContext, directoryTobeCheckedForDelete).size
        if(numOftimestamps == 0) {
          Utility.deleteDirectory(directoryTobeCheckedForDelete, acumeCacheContext)
        }
      })
      
      logger.info("Deleting finished on startup.")
      
    } catch {
      case e: Exception => logger.warn(e.getMessage())
    }
  }

  def checkIfTableAlreadyExist(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    import scala.StringContext._
    try {
      val diskDirectory = Utility.getDiskDirectoryForPoint(acumeCacheContext, cube, levelTimestamp)
      val diskDirpath = new Path(diskDirectory)
	    //Do previous run cleanup
      val priority = Utility.getPriority(levelTimestamp.timestamp, levelTimestamp.level.localId, levelTimestamp.aggregationLevel.localId, cube.diskLevelPolicyMap, BinAvailabilityPoller.getLastBinPersistedTime(cube.binSource))

      if (priority == 1 && Utility.isPathExisting(diskDirpath, acumeCacheContext) && Utility.isDiskWriteComplete(diskDirectory, acumeCacheContext)) {
        acumeCacheContext.cacheSqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk cache reading " + diskDirectory, false)
        val rdd = acumeCacheContext.cacheSqlContext.parquetFileIndivisible(diskDirectory)
        return new AcumeFlatSchemaCacheValue(new AcumeDiskValue(levelTimestamp, cube, rdd, true), acumeCacheContext)
      }
    } catch {
      case _: Exception =>
    }
    null
  }
  
  def get(key: LevelTimestamp) = {
    val cacheValue = cachePointToTable.get(key)
    AcumeCacheContextTraitUtil.addAcumeTreeCacheValue(cacheValue)
    cacheValue
  }
  
  def tryGetOrNull(key : LevelTimestamp) = {
    try {
      key.loadType = LoadType.DISK
      var cacheValue : AcumeTreeCacheValue = null
      cacheValue = cachePointToTable.get(key)
      cacheValue
    } catch {
      case e : java.util.concurrent.ExecutionException => if(e.getCause().isInstanceOf[NoDataException]) null else throw e
    }
  }
  
  def tryGet(key : LevelTimestamp) = {
    tryGetOrNull(key)
  }

  protected def populateParent(childlevel: Long, childTimestamp: Long) {
    val parentSiblingMap = cacheLevelPolicy.getParentSiblingMap(childlevel, childTimestamp)
    for ((parent, children) <- parentSiblingMap) {
      val parentTimestamp = Utility.floorFromGranularity(childTimestamp, parent)
      val parentPoint = tryGetOrNull(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp, LoadType.DISK))
      if (parentPoint == null) {
        var shouldPopulateParent = true
        breakable {
          for (child <- children) {
            val childData = tryGetOrNull(new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child, LoadType.DISK))
            if (childData == null) {
              shouldPopulateParent = false
              break
            }
          }
        }
        if (shouldPopulateParent) {
          val parentData = cachePointToTable.get(new LevelTimestamp(CacheLevel.getCacheLevel(parent), parentTimestamp, LoadType.InMemory))
          populateParent(parent, Utility.floorFromGranularity(childTimestamp, parent))
        }
      }
    }
  }

  /* Method to combine child points to aggregated parent point */
  def mergeChildPoints(emptyRdd: DataFrame, rdds: Seq[DataFrame]): DataFrame = rdds.reduce(_.unionAll(_))

  /* Method to combine child points to a single zipped point containing data of all the points*/
  def zipChildPoints(rdds : Seq[DataFrame]): DataFrame = {
    
    acumeCacheContext.cacheSqlContext.applySchema(
        rdds.map(x => x.rdd).reduce((x, y) => { x.union(y).coalesce(Math.max(x.partitions.size, y.partitions.size), false)}),
        rdds.iterator.next().schema
    )
  }

  
  protected def combineLevels(childlevel: Long, childTimestamp: Long) {
    //TODO check if this level has to be combined at any other level
    val aggregationLevel = cacheLevelPolicy.getAggregationLevel(childlevel)
    if(aggregationLevel == childlevel) {
      return
    }
    logger.info("Combining level {} to {}", childlevel, aggregationLevel)
    val aggregatedDataTimestamp = Utility.floorFromGranularity(childTimestamp, aggregationLevel)
    val aggregatedTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), aggregatedDataTimestamp, LoadType.DISK, CacheLevel.getCacheLevel(aggregationLevel))
    var combinePoint = tryGetOrNull(aggregatedTimestamp)
    val childrenData = scala.collection.mutable.ArrayBuffer[AcumeValue]()
    if (combinePoint == null) {
      var shouldCombine = true
      breakable {
        val children = cacheLevelPolicy.getCombinableIntervals(aggregatedDataTimestamp, aggregationLevel, childlevel)
        logger.info("Children are {}", children)
        for (child <- children) {
          val childLevelTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child, LoadType.DISK)
          val childData = tryGetOrNull(childLevelTimestamp)
          if (childData == null) {
            shouldCombine = false
            logger.info("Not combining, child data is not present {}", childLevelTimestamp)
            break
          } else {
        	  childrenData += childData.getAcumeValue
          }
        }
      }
      if (shouldCombine) {
        var isSuccessCombiningPoint = true
        val context = AcumeTreeCache.getContext(acumeCacheContext.cacheConf.getInt(ConfConstants.schedulerThreadPoolSize).get)
        val f: Future[Option[AcumeFlatSchemaCacheValue]] = Future({
          if (tryGetOrNull(aggregatedTimestamp) == null) {
        	  logger.info("Finally Combining level {} to aggregationlevel " + aggregationLevel + " and levelTimeStamp {} ", childlevel, aggregatedTimestamp)
            acumeCacheContext.cacheSqlContext.sparkContext.setJobGroup(Thread.currentThread().getName + "-" + Thread.currentThread().getId(), "Combining childLevel " + childlevel + " to aggregationlevel " + aggregationLevel, false)
        	  val cachevalue = new AcumeFlatSchemaCacheValue(new AcumeInMemoryValue(aggregatedTimestamp, cube, zipChildPoints(childrenData.map(_.measureSchemaRdd))), acumeCacheContext)
        	  cachePointToTable.put(aggregatedTimestamp, cachevalue)
        	  notifyObserverList
        	  var diskWritingComplete = false;
        	  while(cachevalue.getAcumeValue.isInstanceOf[AcumeInMemoryValue] && !cachevalue.isFailureWritingToDisk) {
        	    Thread.sleep(1000)
        	  }
        	  Some(cachevalue)
          } else {
            logger.info("Already present {}", aggregatedTimestamp)
            None
          }
        })(context)

        f.onComplete {
          case Success(cachevalue) =>
            if(cachevalue != None) {
            	logger.info("Combine writing complete " + aggregatedTimestamp)
            	childrenData.map(x => cachePointToTable.invalidate(x.levelTimestamp))
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

  def mergePathRdds(rdds : Iterable[DataFrame]) = {
    Utility.withDummyCallSite(acumeCacheContext.cacheSqlContext.sparkContext) {
        rdds.reduce(_.unionAll(_))
    }
  }
}

object AcumeTreeCache {

  @transient
  private var context: ExecutionContextExecutorService = null
  
  def getContext(queueSize: Int) = {
    if(context == null) {
      synchronized {
        if(context == null) {
          val executorService = new ThreadPoolExecutor(1, 1,
                                        0L, TimeUnit.MILLISECONDS,
                                        new LinkedBlockingQueue[Runnable](),new NamedThreadPoolFactory("CompactionWriter"));
          context = ExecutionContext.fromExecutorService(executorService)
        }
      }
    }
    context
  }

}

object AcumeCombineUtil {
   def zipTwo(itr: Iterator[Row], other: Iterator[Row]): Iterator[Row] = {
    itr.zip(other).flatMap(x => Seq(x._1, x._2))
  }
}
