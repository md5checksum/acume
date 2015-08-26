package com.guavus.acume.core.executor

import java.util.concurrent.Callable
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.workflow.{AcumeCacheContextTrait, CubeKey}
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

			// get cache points corresponding to this cube
      // default behaviour is timeseries
      val (rdds, acumeCube) = getCubeAndCachePoints(indexDimensionValue, startTime, endTime, gran, cube)

			// execute custom processing part
			response = customExec(rdds, acumeCube)

    } finally {
      HttpUtils.recycle()
      AcumeThreadLocal.unset()
      this.callId = null
    }
    response
  }

  def getCubeAndCachePoints(
      indexDimensionValue: Long,
      startTime: Long,
      endTime: Long,
      gran: TimeGranularity.TimeGranularity,
      cube: CubeKey): (Seq[SchemaRDD], Cube) = {
    acumeCacheContext.getCachePoints(indexDimensionValue, startTime, endTime, gran, cube)
  }

	def customExec(rdds: Seq[SchemaRDD], cube: Cube): T

}