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

abstract case class AcumeTreeCacheValue(dimensionTableName: String = null, acumeContext: AcumeCacheContextTrait) {
  protected var acumeValue: AcumeValue
  def getAcumeValue() = acumeValue
  
  def evictFromMemory
}

class AcumeStarTreeCacheValue(dimensionTableName: String, protected var acumeValue: AcumeValue, acumeContext: AcumeCacheContextTrait) extends AcumeTreeCacheValue(dimensionTableName, acumeContext) {
  def evictFromMemory() = Unit
}

class AcumeFlatSchemaCacheValue(protected var acumeValue: AcumeValue, acumeContext: AcumeCacheContextTrait) extends AcumeTreeCacheValue(null, acumeContext) {
  @volatile 
  var shouldCache = true
  import scala.concurrent._
  import scala.util.{ Success, Failure }

  def evictFromMemory() {
    if(acumeValue.isInstanceOf[AcumeInMemoryValue])
    	shouldCache = false
	 acumeValue.evictFromMemory
  }
  
  val context = AcumeTreeCacheValue.context
  val f: Future[AcumeDiskValue] = Future({
    val diskDirectory = AcumeTreeCacheValue.getDiskDirectoryForPoint(acumeContext, acumeValue.cube, acumeValue.levelTimestamp)
    acumeValue.measureSchemaRdd.saveAsParquetFile(diskDirectory)
    val rdd = acumeContext.cacheSqlContext.parquetFile(diskDirectory)
    new AcumeDiskValue(acumeValue.levelTimestamp, acumeValue.cube, rdd)
  })(context)

  f.onComplete {
    case Success(diskValue) => this.acumeValue = diskValue
    if(!shouldCache) 
      acumeValue.evictFromMemory
    case Failure(t) => //TODO Retry or what since we have lost track of the rdd
  }(context)
}

trait AcumeValue {
  val levelTimestamp: LevelTimestamp
  val cube: Cube
  val measureSchemaRdd: SchemaRDD
  
  def evictFromMemory() {
    measureSchemaRdd.unpersist(false)
  }

  def registerAndCacheDataInMemory() {
    measureSchemaRdd.cache
    measureSchemaRdd.count
  }

}

case class AcumeInMemoryValue(levelTimestamp: LevelTimestamp, cube: Cube, measureSchemaRdd: SchemaRDD) extends AcumeValue {
  registerAndCacheDataInMemory()

}

case class AcumeDiskValue(levelTimestamp: LevelTimestamp, cube: Cube, val measureSchemaRdd: SchemaRDD) extends AcumeValue {
  registerAndCacheDataInMemory()

}

object AcumeTreeCacheValue {
  val executorService = Executors.newFixedThreadPool(2)
  val context = ExecutionContext.fromExecutorService(AcumeTreeCacheValue.executorService)

  
  def getDiskDirectoryForPoint(acumeContext : AcumeCacheContextTrait, cube : Cube, levelTimestamp : LevelTimestamp) = {
    val cacheDirectory = acumeContext.cacheConf.get(ConfConstants.cacheBaseDirectory) + File.separator + acumeContext.cacheSqlContext.sparkContext.getConf.get("spark.app.name") + "-" + acumeContext.cacheConf.get(ConfConstants.cacheDirectory)
    cacheDirectory + File.separator + cube.binsource + File.separator + cube.cubeName + File.separator + levelTimestamp.level + File.separator + levelTimestamp.timestamp
  }
}


