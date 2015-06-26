package com.guavus.acume.cache.core

import java.util.concurrent.Executors

import scala.concurrent._
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import org.apache.spark.sql.SchemaRDD
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.CacheLevel._
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

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
  val context = AcumeTreeCacheValue.context
  var isSuccessWritingToDisk = false
  if(acumeValue.isInstanceOf[AcumeInMemoryValue]) {
    val f: Future[AcumeDiskValue] = Future({
      val diskDirectory = Utility.getDiskDirectoryForPoint(acumeContext, acumeValue.cube, acumeValue.levelTimestamp)
      Utility.deleteDirectory(diskDirectory, acumeContext)
      acumeValue.measureSchemaRdd.sqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk Writing " + diskDirectory, false)
      acumeValue.measureSchemaRdd.saveAsParquetFile(diskDirectory)
      acumeContext.cacheSqlContext.sparkContext.setJobGroup("disk_acume" + Thread.currentThread().getId(), "Disk Reading " + diskDirectory, false)
      val rdd = acumeContext.cacheSqlContext.parquetFileIndivisible(diskDirectory)
      val value = new AcumeDiskValue(acumeValue.levelTimestamp, acumeValue.cube, rdd)
      value.acumeContext = acumeContext
      value
    })(context)

    f.onComplete {
      case Success(diskValue) => {
        logger.info("Disk write complete for {}" + acumeValue.levelTimestamp.toString())
        this.acumeValue = diskValue
        if (!shouldCache) {
        	acumeValue.evictFromMemory
        }
        isSuccessWritingToDisk = true
      }
      case Failure(t) => isSuccessWritingToDisk = false
      logger.error("", t)
    }(context)
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
  var tableName = cube.getAbsoluteCubeName
  tableName = tableName + Utility.getlevelDirectoryName(levelTimestamp.level, levelTimestamp.aggregationLevel)
  tableName = tableName + "_" + levelTimestamp.timestamp + "_temp_memory_only"
  
  registerAndCacheDataInMemory(tableName)
  
  override def registerAndCacheDataInMemory(tableName : String) {
    measureSchemaRdd.registerTempTable(tableName)
    measureSchemaRdd.sqlContext.cacheTable(tableName)
  }
  
  override protected def finalize() {
    logger.info("Unpersisting Data object {} for temp_memory_only ", levelTimestamp)
    evictFromMemory
  }
}

case class AcumeDiskValue(levelTimestamp: LevelTimestamp, cube: Cube, val measureSchemaRdd: SchemaRDD) extends AcumeValue {
  var tableName = cube.getAbsoluteCubeName
  tableName = tableName + Utility.getlevelDirectoryName(levelTimestamp.level, levelTimestamp.aggregationLevel)
  tableName = tableName + "_" + levelTimestamp.timestamp + "_memory_disk"
  
  registerAndCacheDataInMemory(tableName)
  
  override protected def finalize() {
    logger.info("Unpersisting Data object {} for memory as well as disk ", levelTimestamp)
    evictFromMemory
    Utility.deleteDirectory(Utility.getDiskDirectoryForPoint(this.acumeContext, cube, levelTimestamp), acumeContext)
  }

}

object AcumeTreeCacheValue {
  val executorService = Executors.newFixedThreadPool(2)
  val context = ExecutionContext.fromExecutorService(AcumeTreeCacheValue.executorService)
  val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCacheValue])
  
}
