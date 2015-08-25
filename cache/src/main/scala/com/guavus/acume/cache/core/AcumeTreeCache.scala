package com.guavus.acume.cache.core

import java.io.File
import java.util.concurrent.Executors
import scala.collection.JavaConversions._
import scala.concurrent._
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.control.Breaks._
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
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.utility.Utility
import org.apache.spark.sql.SchemaRDD
import org.apache.hadoop.fs.Path
import com.guavus.acume.cache.common.LoadType
import com.guavus.acume.cache.common.ConfConstants
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.rdd.RDD

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import com.guavus.acume.threads.NamedThreadPoolFactory

abstract class AcumeTreeCache(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeCache[LevelTimestamp, AcumeTreeCacheValue](acumeCacheContext, conf, cube) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCache].getName() + "-" + cube.getAbsoluteCubeName)
  
  deleteExtraDataFromDiskAtStartUp
  
  def deleteExtraDataFromDiskAtStartUp() {
    logger.info("Starting deleting file")
    try {
      val cacheBaseDirectory = Utility.getCubeBaseDirectory(acumeCacheContext, cube)
      val path = new Path(cacheBaseDirectory)
      val fs = path.getFileSystem(acumeCacheContext.cacheSqlContext.sparkContext.hadoopConfiguration)
      
      // Get all the levels persisted in diskCache
      val directoryLevelValues = fs.listStatus(path).map(directory => {
    	  val splitPath = directory.getPath().getName().split("-")
    	  if(splitPath.size ==1)
    		  (CacheLevel.nameToLevelMap.get(splitPath(0)).getOrElse(null),CacheLevel.nameToLevelMap.get(splitPath(0)).getOrElse(null))
    	  else
    		  (CacheLevel.nameToLevelMap.get(splitPath(0)).getOrElse(null),CacheLevel.nameToLevelMap.get(splitPath(1)).getOrElse(null))
        })
      
      // Delete all the levels not present in diskLevelPolicyMap
      val notPresentLevels = directoryLevelValues.filter( x => {
        cube.diskLevelPolicyMap.entrySet().filter( level => {level.getKey().level == x._1.localId && level.getKey().aggregationLevel == x._2.localId}).size == 0
      })
      logger.info("Not present levels are {} " + notPresentLevels.map(x=> x._1 + "-" + x._2).mkString(","))
      for (notPresent <- notPresentLevels) {
        val basedir = Utility.getCubeBaseDirectory(acumeCacheContext, cube) 
        val directoryToBeDeleted = Utility.getlevelDirectoryName(notPresent._1, notPresent._2) 
        Utility.deleteDirectory(basedir + File.separator + directoryToBeDeleted, acumeCacheContext)
      }
      
      //>> Evict the evictable timestamps of the levels present in diskLevelPolicyMap
      val presentLevels = directoryLevelValues.filter( x => {
        !(cube.diskLevelPolicyMap.entrySet().filter( level => {level.getKey().level == x._1.localId && level.getKey().aggregationLevel == x._2.localId}).size == 0)
      })
      logger.info("Present levels are " + presentLevels.map(x=> x._1 + "-" + x._2).mkString(","))
      
      for (presentLevel <- presentLevels) {
        val levelDirectoryName = Utility.getlevelDirectoryName(presentLevel._1, presentLevel._2)
        val timestamps = fs.listStatus(new Path(cacheBaseDirectory + File.separator + levelDirectoryName)).map(_.getPath().getName().toLong)
        for (timestamp <- timestamps)
          if (Utility.getPriority(timestamp, presentLevel._1.localId, presentLevel._2.localId, cube.diskLevelPolicyMap, acumeCacheContext.getLastBinPersistedTime(cube.binsource)) == 0) {
            val directoryToBeDeleted = Utility.getDiskDirectoryForPoint(acumeCacheContext, cube, new LevelTimestamp(presentLevel._1, timestamp, presentLevel._2))
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
        val priority = Utility.getPriority(levelTimestamp.timestamp, levelTimestamp.level.localId, levelTimestamp.aggregationLevel.localId, cube.diskLevelPolicyMap, acumeCacheContext.getLastBinPersistedTime(cube.binsource))
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
    AcumeCacheContextTrait.addAcumeTreeCacheValue(cacheValue)
    notifyObserverList
    cacheValue
  }
  
  def tryGet(key : LevelTimestamp) = {
    try {
      key.loadType = LoadType.DISK
      var cacheValue : AcumeTreeCacheValue = null
      cacheValue = cachePointToTable.get(key)
      notifyObserverList
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
  def mergeChildPoints(emptyRdd: SchemaRDD, rdds: Seq[SchemaRDD]): SchemaRDD = rdds.reduce(_.unionAll(_))

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
    val childrenData = scala.collection.mutable.ArrayBuffer[AcumeValue]()
    if (combinePoint == null) {
      var shouldCombine = true
      breakable {
        val children = cacheLevelPolicy.getCombinableIntervals(aggregatedDataTimestamp, aggregationLevel, childlevel)
        logger.info("Children are {}", children)
        for (child <- children) {
          val childLevelTimestamp = new LevelTimestamp(CacheLevel.getCacheLevel(childlevel), child, LoadType.DISK)
          val childData = tryGet(childLevelTimestamp)
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
        val context = AcumeTreeCache.getContext(acumeCacheContext.cacheConf.getInt(ConfConstants.threadPoolSize))
        val f: Future[Option[AcumeFlatSchemaCacheValue]] = Future({
          if (tryGet(aggregatedTimestamp) == null) {
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

  def mergePathRdds(rdds : Iterable[SchemaRDD]) = {
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
