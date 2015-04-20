package com.guavus.acume.core.scheduler

import java.net.ConnectException
import scala.reflect.BeanProperty
import org.apache.thrift.TApplicationException
import org.apache.thrift.TException
import org.apache.thrift.transport.TTransportException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.common.base.Throwables
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.DataService
import scala.util.control.Breaks._
import QueryPrefetchTask._
import com.guavus.acume.workflow.RequestDataType
import com.guavus.rubix.user.management.utils.HttpUtils
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.core.AcumeContext
import java.util.concurrent.TimeoutException

object QueryPrefetchTask {

  val logger = LoggerFactory.getLogger(classOf[QueryPrefetchTask])

}

class QueryPrefetchTask(private var dataService: DataService, @BeanProperty var request: PrefetchTaskRequest, version : Int, taskManager : QueryRequestPrefetchTaskManager, acumeContext : AcumeContextTrait) extends Runnable with Comparable[QueryPrefetchTask] {

  private val RETRY_INTERVAL_IN_MILLIS = acumeContext.acumeConf.getQueryPrefetchTaskRetryIntervalInMillis

  private val MAX_NO_RETRIES = acumeContext.acumeConf.getQueryPrefetchTaskNoOfRetries

  override def run() {
    logger.info("Consuming Task : {}", request)
    var retryIntervalInMillis = RETRY_INTERVAL_IN_MILLIS
    var error: Throwable = null
    var success = false
    var reTryCount = -1
    var flag = false
    breakable{
      while (true){
    	acumeContext.acumeConf.setLocalProperty(ConfConstants.queryTimeOut, String.valueOf(acumeContext.acumeConf.get(ConfConstants.schedulerQueryTimeOut)* (reTryCount + 2)))
        flag = false
        if (version != taskManager.getVersion) {
          logger.info("Not executing older prefetching task as view has changed")
          break;
        }
        reTryCount += 1
        try {
          HttpUtils.setLoginInfo(acumeContext.acumeConf.getSuperUser)
          print(dataService.servRequest(request.toSql("ts")))
          success = true
        } catch {
          case t: Throwable => {
            error = t
            logger.warn("scheduled task failed for " + request + " with retry count : " + reTryCount, t)
          }
        } finally {
          HttpUtils.recycle()
        }
        if (!success && retryExecution(error, reTryCount)) {
          try {
            Thread.sleep(retryIntervalInMillis)
            if (retryIntervalInMillis < 3600000) {
              retryIntervalInMillis *= 2
            }
            logger.info("Going to perform retry no :" + (reTryCount + 1) + " for request " + request)
          } catch {
            case e: InterruptedException => {
              logger.error("Exception while waiting to retry the task " + e)
              Throwables.propagate(e)
            }
          }
          flag = true
        }
        if (!flag) {
          break
        }
      }
    }
    logger.info("Finished Task fetch: {}", request)
  }

  private def retryExecution(error: Throwable, reTryCount: Int): Boolean = {
    if (Utility.isCause(classOf[TException], error) && Utility.isCause(classOf[ConnectException], error)) {
      return true
    }
    if (reTryCount >= MAX_NO_RETRIES) {
      logger.error("scheduled task failed even after " + reTryCount + " retries, for " + request)
      Throwables.propagate(error)
    }
    if (Utility.isCause(classOf[TimeoutException], error)) {
      return true
    }
    false
  }

  override def compareTo(o: QueryPrefetchTask): Int = {
    val equal = 0
    val before = -1
    val after = 1
    val otherEndTime = o.request.getQueryRequest.getEndTime.toInt
    val endTime = request.getQueryRequest.getEndTime.toInt
    val otherStartTime = o.request.getQueryRequest.getStartTime.toInt
    val startTime = request.getQueryRequest.getStartTime.toInt
    if (startTime == otherStartTime && endTime == otherEndTime) {
      if (o.request.getQueryRequest.getSubQuery == null && request.getQueryRequest.getSubQuery == null) {
        if (o.request.getRequestDataType == RequestDataType.TimeSeries) {
          return after
        } else {
          return before
        }
      } else if (o.request.getQueryRequest.getSubQuery != null && request.getQueryRequest.getSubQuery != null) {
        return after
      } else {
        if (request.getQueryRequest.getSubQuery != null && o.request.getQueryRequest.getSubQuery == null) {
          if (o.request.getQueryRequest.getResponseDimensions.containsAll(request.getQueryRequest.getSubQuery.getResponseDimensions)) {
            return after
          } else {
            return after
          }
        } else {
          if (request.getQueryRequest.getResponseDimensions.containsAll(o.request.getQueryRequest.getSubQuery.getResponseDimensions)) {
            return before
          } else {
            return before
          }
        }
      }
    }
    var priority = if ((startTime - otherStartTime) != 0) (otherStartTime - startTime) else (endTime - startTime) - (otherEndTime - otherStartTime)
    if (priority == 0) {
      priority = -1
    }
    priority
  }

  override def toString(): String = {
    "QueryPrefetchTask [request=" + request + "]"
  }
}