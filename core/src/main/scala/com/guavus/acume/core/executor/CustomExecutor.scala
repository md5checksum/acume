package com.guavus.acume.core.executor

import java.util.concurrent.Callable
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.{AcumeCache, TimeGranularity}
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.workflow.{AcumeCacheContextTrait, AcumeCacheContextTraitUtil}
import com.guavus.acume.cache.workflow.CubeKey
import CustomExecutor._

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.rubix.logging.util.LoggingInfoWrapper
import com.guavus.acume.user.management.utils.HttpUtils
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.core.{AcumeConf, AcumeContextTraitUtil, DataService}
import com.guavus.acume.core.configuration.{AcumeContextTraitMap, ConfigFactory}

import com.guavus.qb.ds.DatasourceType

import org.apache.spark.sql.SchemaRDD

object CustomExecutor {

private val logger: Logger = LoggerFactory.getLogger(classOf[CustomExecutor[Any]])

// Get acumeCacheContext for AcumeCache dataSource
// TODO: make this callalbe generic for other data sources. Currently only acume cache is supported
private val acumeCacheContext: AcumeCacheContextTrait =
  ConfigFactory.getInstance.getBean(classOf[AcumeContextTraitMap]).a.get(DatasourceType.CACHE.dsName).get.acc

}

/**
 * Main class that gives a structure to what callable could be passed to acume
 * @param indexDimensionValue value for index dimension filter
 * @param startTime start time for retrieving acume cache values
 * @param endTime end time for retrieving acume cache values
 * @param gran gran for retrieving acume cache values
 * @param cube cube for retrieving acume cache values
 * @tparam T Output type T
 */
abstract class CustomExecutor[T](
    val indexDimensionValue: Long,
    val startTime: Long,
    val endTime: Long,
    val gran: Option[TimeGranularity.TimeGranularity],
    val cube: CubeKey)
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
      
      // gran could be null
      val granVal = if (gran.isDefined) gran.get.getGranularity else 0

	  // get cache points corresponding to this cube
      // default behaviour is timeseries
      val (rdds, timeStampList) = getCachePoints(instance, startTime, endTime, granVal, None, true)

	  // execute custom processing part
	  response = customExec(acumeCacheContext, rdds, timeStampList, instance.cube)

    } catch {
      case e: Throwable =>
        logger.error(s"Cancelling Query for with GroupId " + jobGroupId, e)
        sc.cancelJobGroup(jobGroupId)
        throw e;
    } finally {
      HttpUtils.recycle
      AcumeThreadLocal.unset
      AcumeCacheContextTraitUtil.unsetAcumeTreeCacheValue
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
      gran: Long,
      queryOptionalParam: Option[QueryOptionalParam],
      isMetaData: Boolean): (Seq[SchemaRDD], List[Long]) = {
    instance.getCachePoints(startTime, endTime, gran, None, true)
  }

    def customExec(acumeCacheContext: AcumeCacheContextTrait, rdds: Seq[SchemaRDD], timeStampList: List[Long], cube: Cube): T
}
