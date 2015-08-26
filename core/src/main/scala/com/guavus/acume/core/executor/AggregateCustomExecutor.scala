package com.guavus.acume.core.executor

import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.{AcumeCache, TimeGranularity}
import com.guavus.acume.cache.utility.QueryOptionalParam
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


  override def getCachePoints[k, v](
      instance: AcumeCache[k, v],
      startTime: Long,
      endTime: Long,
      tableName: String,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) = {
    instance.getAggregateCachePoints(startTime, endTime, tableName, None, true)
  }

}