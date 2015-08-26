package com.guavus.acume.core.executor

import java.util.concurrent.Callable
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.{AcumeCache, TimeGranularity}
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.workflow.{AcumeCacheContext, AcumeCacheContextTrait, CubeKey}

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.rubix.logging.util.LoggingInfoWrapper
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.user.management.utils.HttpUtils
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.core.{AcumeContextTrait, AcumeService}
import com.guavus.acume.workflow.RequestDataType
import scala.collection.mutable.HashMap

import org.apache.spark.sql.SchemaRDD

object CustomExecutor {

private var logger: Logger = LoggerFactory.getLogger(classOf[CustomExecutor[Any]])

}

abstract class CustomExecutor[T](
    acumeCacheContext: AcumeCacheContextTrait,
    indexDimensionValue: Long,
    startTime: Long,
    endTime: Long,
    gran: TimeGranularity.TimeGranularity,
    cube: CubeKey)
  extends Callable[T] {

  @BeanProperty
  var callId: String = _
  
  /* var properties: HashMap[String, Any] = null
  
  def setProperties(property: HashMap[String, Any]) {
    properties = property
  } */
  
  final def call(): T = {
    var response: T = null.asInstanceOf[T]
    val LoggingInfoWrapper = new LoggingInfoWrapper()
    LoggingInfoWrapper.setTransactionId(this.callId)
    AcumeThreadLocal.set(LoggingInfoWrapper)
    try {

      // Get cache instance
      val instance = getCacheInstance(indexDimensionValue, startTime, endTime, cube)

			// get cache points corresponding to this cube
      // default behaviour is timeseries
      val tableName = AcumeCacheContext.getTable(cube.name)
      val (rdds, timeStampList) = getCachePoints(instance, startTime, endTime, tableName, None, true)

			// execute custom processing part
			response = customExec(rdds, timeStampList, instance.cube)

    } finally {
      HttpUtils.recycle()
      AcumeThreadLocal.unset()
      this.callId = null
    }
    response
  }

  def getCacheInstance[k, v](
      indexDimensionValue: Long,
      startTime: Long,
      endTime: Long,
      cube: CubeKey): AcumeCache[k, v] = {

    acumeCacheContext.getCacheInstance(indexDimensionValue, startTime, endTime, cube)
  }

  def getCachePoints[k, v](
      instance: AcumeCache[k, v],
      startTime: Long,
      endTime: Long,
      tableName: String,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) = {
    instance.getCachePoints(startTime, endTime, tableName, None, true)
  }

    def customExec(rdds: Seq[SchemaRDD], timeStampList: List[Long], cube: Cube): T
}