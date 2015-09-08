package com.guavus.acume.core.executor

import java.util.concurrent.Callable
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.{AcumeCache, TimeGranularity}
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.workflow.{AcumeCacheContextTrait, CubeKey}
import CustomExecutor._

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.rubix.logging.util.LoggingInfoWrapper
import com.guavus.acume.user.management.utils.HttpUtils
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.core.{AcumeConf, AcumeContextTraitUtil, DataService}

import org.apache.spark.sql.SchemaRDD

object CustomExecutor {

private var logger: Logger = LoggerFactory.getLogger(classOf[CustomExecutor[Any]])

}

/**
 * Main class that gives a structure to what callable could be passed to acume
 * @param acumeCacheContext
 * @param indexDimensionValue value for index dimension filter
 * @param startTime start time for retrieving acume cache values
 * @param endTime end time for retrieving acume cache values
 * @param gran gran for retrieving acume cache values
 * @param cube cube for retrieving acume cache values
 * @tparam T Output type T
 */
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
    val sc = AcumeContextTraitUtil.sparkContext
    val acumeConf = AcumeContextTraitUtil.acumeConf
 
    val LoggingInfoWrapper = new LoggingInfoWrapper()
    LoggingInfoWrapper.setTransactionId(this.callId)

    val jobDescription =  Array[String]("[" + HttpUtils.getLoginInfo() + "]").mkString("-")

    logger.info(jobDescription)

    val jobGroupId = Thread.currentThread().getName() + "-" +
      Thread.currentThread().getId() + "-" + DataService.counter.getAndIncrement

    AcumeConf.setConf(acumeConf)

    AcumeThreadLocal.set(LoggingInfoWrapper)
    sc.setJobGroup(jobGroupId, jobDescription, false)
    try {

      // Validate query and get cache instance
      val instance = getCacheInstance(startTime, endTime, cube)

	  // get cache points corresponding to this cube
      // default behaviour is timeseries
      val (rdds, timeStampList) = getCachePoints(instance, startTime, endTime, None, true)

	  // execute custom processing part
	  response = customExec(rdds, timeStampList, instance.cube)

    } catch {
      case e: Throwable =>
        logger.error(s"Cancelling Query for with GroupId " + jobGroupId, e)
        sc.cancelJobGroup(jobGroupId)
        throw e;
    } finally {
      HttpUtils.recycle()
      AcumeThreadLocal.unset()
      this.callId = null
    }
    response
  }

  def getCacheInstance[k, v](
      startTime: Long,
      endTime: Long,
      cube: CubeKey): AcumeCache[k, v] = {

    acumeCacheContext.getCacheInstance(startTime, endTime, cube)
  }

  def getCachePoints[k, v](
      instance: AcumeCache[k, v],
      startTime: Long,
      endTime: Long,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) = {
    instance.getCachePoints(startTime, endTime, None, true)
  }

    def customExec(rdds: Seq[SchemaRDD], timeStampList: List[Long], cube: Cube): T
}
