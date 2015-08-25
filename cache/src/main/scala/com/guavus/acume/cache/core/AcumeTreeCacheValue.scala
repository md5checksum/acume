package com.guavus.acume.cache.core

import java.util.concurrent.Executors

import scala.concurrent._
import scala.concurrent.Future

import org.apache.spark.sql.SchemaRDD
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.CacheLevel._
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import scala.util.{ Success, Failure }
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.ConfConstants
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import org.slf4j.Logger

import com.guavus.acume.threads.NamedThreadPoolFactory

import AcumeTreeCacheValue._

abstract case class AcumeTreeCacheValue(dimensionTableName: String = null, acumeContext: AcumeCacheContextTrait) {
  
  protected var acumeValue: AcumeValue
  def getAcumeValue() = acumeValue
  var isInMemory : Boolean
  
  def evictFromMemory
}

class AcumeStarTreeCacheValue(dimensionTableName: String, protected var acumeValue: AcumeValue, acumeContext: AcumeCacheContextTrait) extends AcumeTreeCacheValue(dimensionTableName, acumeContext) {
  def evictFromMemory() = Unit
  var isInMemory = true
}

class AcumeFlatSchemaCacheValue(protected var acumeValue: AcumeValue, acumeContext: AcumeCacheContextTrait) extends AcumeTreeCacheValue(null, acumeContext) {
  @volatile
  var shouldCache = true
  var isInMemory = true
  import scala.concurrent._
  acumeValue.acumeContext = acumeContext
  import scala.util.{ Success, Failure }

  def evictFromMemory() {
    if (acumeValue.isInstanceOf[AcumeInMemoryValue])
      shouldCache = false
    acumeValue.evictFromMemory
    isInMemory = false
  }

  acumeValue.acumeContext = acumeContext
  val context = AcumeTreeCacheValue.getContext(acumeContext.cacheConf.getInt(ConfConstants.threadPoolSize))
  var isFailureWritingToDisk = false
  
  if(acumeValue.isInstanceOf[AcumeInMemoryValue]) {
    val f: Future[AcumeDiskValue] = Future({
      
      try {
        var value : AcumeDiskValue = null
        val diskDirectory = Utility.getDiskDirectoryForPoint(acumeContext, acumeValue.cube, acumeValue.levelTimestamp)
        Utility.deleteDirectory(diskDirectory, acumeContext)
      
        // Check if the point is outside the diskLevelPolicyMap
        val levelTimeStamp = acumeValue.levelTimestamp
        val cube = acumeValue.cube
        val priority = Utility.getPriority(levelTimeStamp.timestamp, levelTimeStamp.level.localId, levelTimeStamp.aggregationLevel.localId, cube.diskLevelPolicyMap, acumeContext.getLastBinPersistedTime(cube.binsource))
      
        // if the timestamp lies in the disk cache range then only write it to disk. Else not.
        if(priority != 0) {
          acumeContext.cacheSqlContext.sparkContext.setLocalProperty("spark.scheduler.pool", "scheduler")
          acumeContext.cacheSqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk Writing " + diskDirectory, false)
          acumeValue.measureSchemaRdd.saveAsParquetFile(diskDirectory)
          acumeContext.cacheSqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk Reading " + diskDirectory, false)
          val rdd = acumeContext.cacheSqlContext.parquetFileIndivisible(diskDirectory)
          value = new AcumeDiskValue(acumeValue.levelTimestamp, acumeValue.cube, rdd)
          value.acumeContext = acumeContext
          logger.info("Disk write complete for {}" + acumeValue.levelTimestamp.toString() + " for cube " + cube.getAbsoluteCubeName)
          this.acumeValue = value
          if (!shouldCache) {
            acumeValue.evictFromMemory
          }
        }
        value
      } catch {
        case ex:Exception => logger.error("Failure creating AcumeDiskValue", ex)
        isFailureWritingToDisk = true
        null
      }
      
    })(context)
    
  }
}

trait AcumeValue {
  val levelTimestamp: LevelTimestamp
  val cube: Cube
  val measureSchemaRdd: SchemaRDD
  var acumeContext : AcumeCacheContextTrait = null
  val logger: Logger = LoggerFactory.getLogger(classOf[AcumeValue])
  val skipCount: Boolean = false
  
  def evictFromMemory() {
    try {
    	measureSchemaRdd.unpersist(true)
    } catch {
      case e : Exception =>
    }
  }

  def registerAndCacheDataInMemory(tableName: String) {
    measureSchemaRdd.registerTempTable(tableName)
    measureSchemaRdd.sqlContext.cacheTable(tableName)
    if(!skipCount) {
      measureSchemaRdd.sqlContext.table(tableName).count
    }
  }
}

case class AcumeInMemoryValue(levelTimestamp: LevelTimestamp, cube: Cube, measureSchemaRdd: SchemaRDD, parentPoints: Seq[(AcumeValue, SchemaRDD)] = Seq()) extends AcumeValue {
  val tempTables = AcumeCacheContextTrait.getInstaTempTable()

  var tableName = cube.getAbsoluteCubeName
  tableName = tableName + Utility.getlevelDirectoryName(levelTimestamp.level, levelTimestamp.aggregationLevel)
  tableName = tableName + "_" + levelTimestamp.timestamp + "_temp_memory_only"
  
  registerAndCacheDataInMemory(tableName)
  
  override def registerAndCacheDataInMemory(tableName : String) {
    measureSchemaRdd.registerTempTable(tableName)
    measureSchemaRdd.sqlContext.cacheTable(tableName)
  }
  
  override protected def finalize() {
    try {
      logger.info("Unpersisting Data object {} for temp_memory_only for cube " + cube.getAbsoluteCubeName, levelTimestamp)
      logger.info("Dropping temp tables {}", tempTables.mkString(","))
      evictFromMemory
      tempTables match {
        case Some(table) => table.asInstanceOf[scala.collection.mutable.ArrayBuffer[String]].map(x => measureSchemaRdd.sqlContext.dropTempTable(x))
        case None =>
      }
      measureSchemaRdd.sqlContext.dropTempTable(tableName)
    } catch {
      case e: Exception => logger.error("", e)
      case e: Throwable => logger.error("", e)
    }
  }
}

case class AcumeDiskValue(levelTimestamp: LevelTimestamp, cube: Cube, val measureSchemaRdd: SchemaRDD, override val skipCount: Boolean = false) extends AcumeValue {
  var tableName = cube.getAbsoluteCubeName
  tableName = tableName + Utility.getlevelDirectoryName(levelTimestamp.level, levelTimestamp.aggregationLevel)
  tableName = tableName + "_" + levelTimestamp.timestamp + "_memory_disk"
  
  registerAndCacheDataInMemory(tableName)
  
  override protected def finalize() {
    logger.info("Unpersisting Data object " + levelTimestamp + " for cube " + cube.getAbsoluteCubeName +" from memory as well as disk ")
    evictFromMemory
    Utility.deleteDirectory(Utility.getDiskDirectoryForPoint(this.acumeContext, cube, levelTimestamp), acumeContext)
    measureSchemaRdd.sqlContext.dropTempTable(tableName)
  }

}

class LimitedQueue[Runnable](maxSize: Int) extends LinkedBlockingQueue[Runnable](maxSize) {

  override def offer(e: Runnable): Boolean = {
    try {
      put(e)
      return true
    } catch {
      case ie: InterruptedException => Thread.currentThread().interrupt()
    }
    false
  }
}

object AcumeTreeCacheValue {
  
	val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCacheValue])
  val DISK_CACHE_WRITER_THREADS = 2
  
  @transient
  private var context: ExecutionContextExecutorService = null
  
  def getContext(queueSize: Int) = {
    
    if(context == null) {
      synchronized {
        if(context == null) {
          val executorService = new ThreadPoolExecutor(DISK_CACHE_WRITER_THREADS, DISK_CACHE_WRITER_THREADS,
                                        0L, TimeUnit.MILLISECONDS,
                                        new LimitedQueue[Runnable](queueSize),new NamedThreadPoolFactory("DiskCacheWriter"));
          context = ExecutionContext.fromExecutorService(executorService)
        }
      }
    }
    context
  }
}
