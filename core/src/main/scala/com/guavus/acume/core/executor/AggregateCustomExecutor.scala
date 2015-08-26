package com.guavus.acume.core.executor

import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.workflow.{AcumeCacheContextTrait, CubeKey}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.rubix.logging.util.LoggingInfoWrapper
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.user.management.utils.HttpUtils
import CustomExecutor._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.core.{AcumeContextTrait, AcumeService}
import com.guavus.acume.workflow.RequestDataType
import scala.collection.mutable.HashMap

import org.apache.spark.sql.SchemaRDD

object AggregateCustomExecutor {

private var logger: Logger = LoggerFactory.getLogger(classOf[CustomExecutor[Any]])

}

abstract class AggregateCustomExecutor[T](
    acumeCacheContext: AcumeCacheContextTrait,
    indexDimensionValue: Long,
    startTime: Long,
    endTime: Long,
    gran: TimeGranularity.TimeGranularity,
    cube: CubeKey)
  extends CustomExecutor[T](acumeCacheContext, indexDimensionValue, startTime, endTime, gran, cube) {


  override def getCubeAndCachePoints(
      indexDimensionValue: Long,
      startTime: Long,
      endTime: Long,
      gran: TimeGranularity.TimeGranularity,
      cube: CubeKey): (Seq[SchemaRDD], Cube) = {
    acumeCacheContext.getAggregateCachePoints(indexDimensionValue, startTime, endTime, gran, cube)
  }

}