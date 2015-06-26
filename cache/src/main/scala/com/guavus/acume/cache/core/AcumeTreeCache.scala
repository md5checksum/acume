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
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.rdd.RDD

abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCache])
  
  deleteExtraDataFromDiskAtStartUp
  
  def deleteExtraDataFromDiskAtStartUp() {
    
    logger.info("Starting deleting file")
    try {
      val cacheBaseDirectory = Utility.getCubeBaseDirectory(acumeCacheContext, cube)
      val path = new Path(cacheBaseDirectory)
      val fs = path.getFileSystem(acumeCacheContext.cacheSqlContext.sparkContext.hadoopConfiguration)
      
      // Get all the levels persisted in diskCache
      val directoryLevelValues = fs.listStatus(path).map(directory => { new Level(directory.getPath().getName()) })
      
      // Delete all the levels not present in diskLevelPolicyMap
      val notPresentLevels = directoryLevelValues.filter(!cube.diskLevelPolicyMap.contains(_))
      logger.info("Not present levels are " + notPresentLevels.mkString(","))
      for (notPresent <- notPresentLevels) {
        val levelDirectoryName = Utility.getlevelDirectoryName(CacheLevel.getCacheLevel(notPresent.level.longValue()), CacheLevel.getCacheLevel(notPresent.aggregationLevel.longValue()))
        val directoryToBeDeleted = cacheBaseDirectory + File.separator + levelDirectoryName 
        Utility.deleteDirectory(directoryToBeDeleted, acumeCacheContext)
      }
      
      // Evict the evictable timestamps of the levels present in diskLevelPolicyMap
      val presentLevels = directoryLevelValues.filter(cube.diskLevelPolicyMap.contains(_))
      logger.info("Present levels are " + presentLevels.mkString(","))
      for (presentLevel <- presentLevels) {
        val levelDirectoryName = Utility.getlevelDirectoryName(CacheLevel.getCacheLevel(presentLevel.level.longValue()), CacheLevel.getCacheLevel(presentLevel.aggregationLevel.longValue()))
        val timestamps = fs.listStatus(new Path(cacheBaseDirectory + File.separator + levelDirectoryName)).map(_.getPath().getName().toLong)
        for (timestamp <- timestamps)
          if (Utility.getPriority(timestamp, presentLevel.level, presentLevel.aggregationLevel, cube.diskLevelPolicyMap, acumeCacheContext.getLastBinPersistedTime(cube.binsource)) == 0) {
            val directoryToBeDeleted = cacheBaseDirectory + File.separator + levelDirectoryName + File.separator + timestamp
            Utility.deleteDirectory(directoryToBeDeleted, acumeCacheContext)
          }
      }
      
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
	    if (Utility.isPathExisting(diskDirpath, acumeCacheContext) && Utility.isDiskWriteComplete(diskDirectory, acumeCacheContext)) {
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

  /* Method to combine child points to aggregated parent point */
  def mergeChildPoints(rdds: Seq[SchemaRDD]): SchemaRDD = rdds.reduce(_.unionAll(_))

  /* Method to combine child points to a single zipped point containing data of all the points*/
  def zipChildPoints(rdds : Seq[SchemaRDD]): SchemaRDD = {
    
    acumeCacheContext.cacheSqlContext().applySchema(
        rdds.map(x => x.asInstanceOf[RDD[Row]]).reduce((x, y) => { x.union(y).coalesce(Math.max(x.partitions.size, y.partitions.size), false)}),
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
            Some(new AcumeFlatSchemaCacheValue(new AcumeInMemoryValue(aggregatedTimestamp, cube, zipChildPoints(childrenData.map(_.getAcumeValue.measureSchemaRdd))), acumeCacheContext))
          } else {
            logger.info("Already present {}", aggregatedTimestamp)
            None
          }
        })(context)

        f.onComplete {
          case Success(cachevalue) =>
            if(cachevalue != None) {
            	logger.info("Combine writing complete " + aggregatedTimestamp)
              
              //Put the combined point to cachePointToTable
              logger.info("Putting combined point to memory")
              cachePointToTable.put(aggregatedTimestamp, cachevalue.get)
    
              //Combining successful. Evict the child points from memory 
            	childrenData.map(_.evictFromMemory)
            	
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
  @transient
  val executorService = Executors.newFixedThreadPool(1)
  @transient
  val context = ExecutionContext.fromExecutorService(AcumeTreeCache.executorService)

}

object AcumeCombineUtil {
   def zipTwo(itr: Iterator[Row], other: Iterator[Row]): Iterator[Row] = {
    itr.zip(other).flatMap(x => Seq(x._1, x._2))
  }
}



