package com.guavus.acume.cache.core

import org.apache.spark.sql.SchemaRDD
import scala.concurrent.Future
import scala.concurrent._
import scala.util.{ Success, Failure }
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
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
  import scala.util.{ Success, Failure }

  def evictFromMemory() {
    if (acumeValue.isInstanceOf[AcumeInMemoryValue])
      shouldCache = false
    acumeValue.evictFromMemory
    isInMemory = false
  }

  acumeValue.acumeContext = acumeContext
  val context = AcumeTreeCacheValue.getContext(acumeContext.cacheConf.getInt(ConfConstants.threadPoolSize))
  var isSuccessWritingToDisk = false
  if(acumeValue.isInstanceOf[AcumeInMemoryValue]) {
    val f: Future[AcumeDiskValue] = Future({
      val diskDirectory = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeContext, acumeValue.cube, acumeValue.levelTimestamp)
      val path = new Path(diskDirectory)
      val fs = path.getFileSystem(acumeContext.cacheSqlContext.sparkContext.hadoopConfiguration)
      fs.delete(path, true)
      acumeContext.cacheSqlContext.sparkContext.setLocalProperty("spark.scheduler.pool", "scheduler")
      acumeContext.cacheSqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk Writing " + diskDirectory, false)
      acumeValue.measureSchemaRdd.saveAsParquetFile(diskDirectory)
      acumeContext.cacheSqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk Reading " + diskDirectory, false)
      val rdd = acumeContext.cacheSqlContext.parquetFileIndivisible(diskDirectory)
      val value = new AcumeDiskValue(acumeValue.levelTimestamp, acumeValue.cube, rdd)
      value.acumeContext = acumeContext
      logger.info("Disk write complete for {}" + acumeValue.levelTimestamp.toString())
      this.acumeValue = value
      if (!shouldCache) {
        acumeValue.evictFromMemory
      }
      isSuccessWritingToDisk = true
      value
    })(context)

  }
}

trait AcumeValue {
  val levelTimestamp: LevelTimestamp
  val cube: Cube
  val measureSchemaRdd: SchemaRDD
  var acumeContext : AcumeCacheContextTrait = null
  val logger: Logger = LoggerFactory.getLogger(classOf[AcumeValue])
  
  def evictFromMemory() {
    try {
    	measureSchemaRdd.unpersist(true)
    } catch {
      case e : Exception =>
    }
  }

  def registerAndCacheDataInMemory(tableName : String) {
    measureSchemaRdd.registerTempTable(tableName)
    measureSchemaRdd.sqlContext.cacheTable(tableName)
    measureSchemaRdd.sqlContext.table(tableName).count
  }

}

case class AcumeInMemoryValue(levelTimestamp: LevelTimestamp, cube: Cube, measureSchemaRdd: SchemaRDD) extends AcumeValue {
  val tempTables = AcumeCacheContextTrait.getInstaTempTable()
  val tableName = cube.getAbsoluteCubeName + levelTimestamp.level + levelTimestamp.timestamp + "_temp_memory_only"
  registerAndCacheDataInMemory(tableName)
  override def registerAndCacheDataInMemory(tableName : String) {
    measureSchemaRdd.registerTempTable(tableName)
    measureSchemaRdd.sqlContext.cacheTable(tableName)
  }
  
  override protected def finalize() {
    try {
    logger.info("Unpersisting Data object {} for temp_memory_only ", levelTimestamp)
    logger.info("Dropping temp tables {}", tempTables.mkString(","))
    evictFromMemory
    tempTables.get.asInstanceOf[scala.collection.mutable.ArrayBuffer[String]].map(x => measureSchemaRdd.sqlContext.dropTempTable(x))
    measureSchemaRdd.sqlContext.dropTempTable(tableName)
    }catch {
    case e: Exception => logger.error("", e)
    case e: Throwable => logger.error("", e)
    }
  }
}

case class AcumeDiskValue(levelTimestamp: LevelTimestamp, cube: Cube, val measureSchemaRdd: SchemaRDD) extends AcumeValue {
   val tableName = cube.getAbsoluteCubeName + levelTimestamp.level + levelTimestamp.timestamp + "_memory_disk"
  registerAndCacheDataInMemory(tableName)
  
  override protected def finalize() {
    logger.info("Unpersisting Data object " + levelTimestamp + " for cube " + cube.getAbsoluteCubeName +" from memory as well as disk ")
    evictFromMemory
    AcumeTreeCacheValue.deleteDirectory(AcumeTreeCacheValue.getDiskDirectoryForPoint(this.acumeContext, cube, levelTimestamp), acumeContext)
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
  
  val DISK_CACHE_WRITER_THREADS = 2
  
  private var context: ExecutionContextExecutorService = null
  
  def getContext(queueSize: Int) = {
    synchronized {
      if(context == null) {
        val executorService = new ThreadPoolExecutor(DISK_CACHE_WRITER_THREADS, DISK_CACHE_WRITER_THREADS,
                                      0L, TimeUnit.MILLISECONDS,
                                      new LimitedQueue[Runnable](queueSize),new NamedThreadPoolFactory("DiskCacheWriter"));
        context = ExecutionContext.fromExecutorService(executorService)
      }
    }
    context
  }

  val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCacheValue])
  
  def deleteDirectory(dir : String, acumeContext : AcumeCacheContextTrait) {
    logger.info("Deleting directory " + dir)
    val path = new Path(dir)
    val fs = path.getFileSystem(acumeContext.cacheSqlContext.sparkContext.hadoopConfiguration)
    fs.delete(path, true)
  }

  def isPathExisting(path : Path, acumeContext : AcumeCacheContextTrait) : Boolean = {
    logger.debug("Checking if path exists => {}", path)
    val fs = path.getFileSystem(acumeContext.cacheSqlContext.sparkContext.hadoopConfiguration)
    return fs.exists(path)
  }
  
  def isDiskWriteComplete(diskDirectory : String, acumeContext : AcumeCacheContextTrait) : Boolean = {
    val path =  new Path(diskDirectory + File.separator + "_SUCCESS")
    isPathExisting(path, acumeContext)
  }

  def getDiskDirectoryForPoint(acumeContext : AcumeCacheContextTrait, cube : Cube, levelTimestamp : LevelTimestamp) = {
    val cacheDirectory = acumeContext.cacheConf.get(ConfConstants.cacheBaseDirectory) + File.separator + acumeContext.cacheSqlContext.sparkContext.getConf.get("spark.app.name") + "-" + acumeContext.cacheConf.get(ConfConstants.cacheDirectory)
    cacheDirectory + File.separator + cube.binsource + File.separator + cube.cubeName + File.separator + levelTimestamp.level + File.separator + levelTimestamp.timestamp
  }
}