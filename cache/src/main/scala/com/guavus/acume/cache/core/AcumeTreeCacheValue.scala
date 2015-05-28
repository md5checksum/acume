package com.guavus.acume.cache.core

import org.apache.spark.sql.SchemaRDD
import scala.concurrent.Future
import scala.concurrent._
import scala.util.{ Success, Failure }
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.ConfConstants
import java.io.File
import com.guavus.acume.cache.common.LevelTimestamp
import java.util.concurrent.Executors
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.LevelTimestamp
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import org.slf4j.Logger
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
  val context = AcumeTreeCacheValue.context
  var isSuccessWritingToDisk = false
  if(acumeValue.isInstanceOf[AcumeInMemoryValue]) {
    val f: Future[AcumeDiskValue] = Future({
      val diskDirectory = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeContext, acumeValue.cube, acumeValue.levelTimestamp)
      val path = new Path(diskDirectory)
      val fs = path.getFileSystem(acumeContext.cacheSqlContext.sparkContext.hadoopConfiguration)
      fs.delete(path, true)
      acumeValue.measureSchemaRdd.saveAsParquetFile(diskDirectory)
      val rdd = acumeContext.cacheSqlContext.parquetFileIndivisible(diskDirectory)
      val value = new AcumeDiskValue(acumeValue.levelTimestamp, acumeValue.cube, rdd)
      value.acumeContext = acumeContext
      value
    })(context)

    f.onComplete {
      case Success(diskValue) => {
        this.acumeValue = diskValue
        if (!shouldCache)
          acumeValue.evictFromMemory
        isSuccessWritingToDisk = true
      }
      case Failure(t) => isSuccessWritingToDisk = false
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
  val tableName = cube.getAbsoluteCubeName + levelTimestamp.level + levelTimestamp.timestamp + "_temp_memory_only"
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
   val tableName = cube.getAbsoluteCubeName + levelTimestamp.level + levelTimestamp.timestamp + "_memory_disk"
  registerAndCacheDataInMemory(tableName)
  
  override protected def finalize() {
    logger.info("Unpersisting Data object {} for memory as well as disk ", levelTimestamp)
    evictFromMemory
    AcumeTreeCacheValue.deleteDirectory(AcumeTreeCacheValue.getDiskDirectoryForPoint(this.acumeContext, cube, levelTimestamp), acumeContext)
  }

}

object AcumeTreeCacheValue {
  val executorService = Executors.newFixedThreadPool(2)
  val context = ExecutionContext.fromExecutorService(AcumeTreeCacheValue.executorService)

  val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCacheValue])
  
  def deleteDirectory(dir : String, acumeContext : AcumeCacheContextTrait) {
    val path = new Path(dir)
    val fs = path.getFileSystem(acumeContext.cacheSqlContext.sparkContext.hadoopConfiguration)
    fs.delete(path, true)
  }
  
  def getDiskDirectoryForPoint(acumeContext : AcumeCacheContextTrait, cube : Cube, levelTimestamp : LevelTimestamp) = {
    val cacheDirectory = acumeContext.cacheConf.get(ConfConstants.cacheBaseDirectory) + File.separator + acumeContext.cacheSqlContext.sparkContext.getConf.get("spark.app.name") + "-" + acumeContext.cacheConf.get(ConfConstants.cacheDirectory)
    cacheDirectory + File.separator + cube.binsource + File.separator + cube.cubeName + File.separator + levelTimestamp.level + File.separator + levelTimestamp.timestamp
  }
}