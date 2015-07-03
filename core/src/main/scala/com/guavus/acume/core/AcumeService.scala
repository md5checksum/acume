package com.guavus.acume.core

import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.rubix.query.remote.flex.AggregateResponse
import com.guavus.rubix.query.remote.flex.QueryRequest
import com.guavus.rubix.query.remote.flex.SearchRequest
import com.guavus.rubix.query.remote.flex.SearchResponse
import com.guavus.rubix.query.remote.flex.TimeseriesResponse
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import javax.xml.bind.annotation.XmlRootElement
import com.google.common.collect.Lists
import com.guavus.acume.core.query.DataExportRequest
import com.guavus.acume.core.query.DataExportResponse
import com.guavus.acume.core.query.CSVDataExporter
import com.guavus.acume.core.query.DataExportResponse
import com.guavus.acume.core.query.DataExporterUtil
import com.guavus.rubix.user.management.exceptions.RubixExceptionConstants
import com.guavus.acume.core.query.IDataExporter
import com.guavus.acume.core.query.DataExportResponse
import com.guavus.acume.core.query.FILE_TYPE
import com.guavus.acume.cache.common.AcumeConstants
import org.apache.commons.lang.exception.ExceptionUtils
import com.guavus.acume.core.webservice.BadRequestException
import com.guavus.acume.core.webservice.HttpError
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.ResponseBuilder
import com.guavus.acume.core.exceptions.AcumeExceptionConstants
import com.guavus.acume.workflow.RequestDataType
import scala.collection.mutable.HashMap
import java.util.concurrent.Future
import com.guavus.acume.threads.QueryExecutorThreads
import com.guavus.rubix.logging.util.LoggingInfoWrapper
import com.guavus.rubix.logging.util.AcumeThreadLocal
import java.util.concurrent.atomic.AtomicInteger
import com.guavus.rubix.logging.util.LoggingInfoWrapper
import com.guavus.rubix.query.remote.flex.QueryExecutor
import com.guavus.acume.user.management.utils.HttpUtils
import com.guavus.acume.cache.utility.Utility
import com.google.common.base.Function
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException
import acume.exception.AcumeException
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import scala.concurrent._
import scala.concurrent.duration._
import com.guavus.acume.cache.common.ConfConstants
import ExecutionContext.Implicits.global

/**
 * Main service of acume which serves the request from UI and rest services. It checks if the response is present in RR cache otherwise fire the query on OLAP cache.
 */
class AcumeService(dataService: DataService) {
  
  private val counter = new AtomicInteger(1);
  
  def this() = {
    this(ConfigFactory.getInstance.getBean(classOf[DataService]))
  }
  
  /**
   * Any query request can be fired and result of appropriate type is returned, If query is of type aggregate it returns aggregate response else timeseries response.
   */
  def servQuery(queryRequest : QueryRequest) {
    // call the query builder and convert query in standard sql-92 format. After that fires this query on to OLAP cache and materialize the RDD and populate the reponse.
  }
  
  private def setCallId(request : Any) {
		var callModuleId = List("MODULE");
    if (request.isInstanceOf[QueryRequest]) {
      val queryRequest = request.asInstanceOf[QueryRequest]
  		if(queryRequest.getParamMap() !=null ){
  			callModuleId = queryRequest.getParamMap().filter(_.getName.equals("M_ID")).map(_.getValue()).toList
  			if(callModuleId==null || callModuleId.size==0) {
  				callModuleId =List(LoggingInfoWrapper.NO_MODULE_ID);
  			}
  		}
    }

		val id = this.counter.getAndIncrement();
		val loggingInfoWrapper = new LoggingInfoWrapper();
		loggingInfoWrapper.setTransactionId(callModuleId(0)+"-"+id+"-");
		AcumeThreadLocal.set(loggingInfoWrapper);
	}

  private def servMultiple[T](callableResponses: java.util.ArrayList[Callable[T]], requests: java.util.ArrayList[_ <: Any], requestDataType: RequestDataType.RequestDataType, checkJobProperty: Boolean, classificationList: List[String], poolList: List[String]): java.util.ArrayList[T] = {

    val starttime = System.currentTimeMillis()

    def runWithTimeout[T](f: => java.util.ArrayList[T]): java.util.ArrayList[T] = {
      lazy val fut = future { f }
      Await.result(fut, DurationInt(dataService.acumeContext.acumeConf.getInt(ConfConstants.queryTimeOut, 30)) second)
    }

    def run() = {

      val futureResponses = new java.util.ArrayList[Future[T]]();
      val threadPool = QueryExecutorThreads.getPool();
      val isIDSet = false;

      callableResponses foreach (callableResponse => {
        futureResponses.add(threadPool.submit(callableResponse))
      })

      val responses = new java.util.ArrayList[T]()
      val classificationIterator = classificationList.iterator
      val poolIterator = poolList.iterator

      try {
        for (futureResponse <- futureResponses) {
          try {
            responses.add(futureResponse.get())
            if (checkJobProperty && poolIterator.hasNext)
              dataService.queryPoolUIPolicy.updateStats(poolIterator.next(), classificationIterator.next(), dataService.poolStats, dataService.classificationStats, starttime, System.currentTimeMillis())
          } catch {
            case e: ExecutionException => {
              Utility.throwIfRubixException(e)
              //TO DO print the exact query which throw exception
              throw new RuntimeException("Exception encountered while getting response for ", e)
            }
            case e: InterruptedException => {
              Utility.throwIfRubixException(e);
              //TO DO print the exact query which throw exception
              throw new RuntimeException("Exception encountered while getting response for ", e);
            }
          }
        }
      } finally {
        if (checkJobProperty) {
          while (poolIterator.hasNext) {
            dataService.queryPoolUIPolicy.updateStats(poolIterator.next(), classificationIterator.next(), dataService.poolStats, dataService.classificationStats, starttime, System.currentTimeMillis())
          }
          dataService.queryPoolUIPolicy.updateFinalStats(poolList.iterator.next(), classificationList.iterator.next(), dataService.poolStats, dataService.classificationStats, starttime, System.currentTimeMillis())
        }
      }
      responses
    }
    runWithTimeout[T](run())
  }
  
  def checkJobPropertiesAndUpdateStats(requests: java.util.ArrayList[_ <: Any], requestDataType: RequestDataType.RequestDataType): (List[(String, HashMap[String, Any])], List[String]) = {
    var poolname: String = null
    var classificationname: String = null
    this.synchronized {
      var classificationandpool = dataService.checkJobLevelProperties(requests, requestDataType)
      dataService.queryPoolUIPolicy.updateInitialStats(classificationandpool._2, classificationandpool._1.map(x => x._1), dataService.poolStats, dataService.classificationStats)
      classificationandpool
    }
  }
  
  private def servMultiple[T](requestDataType: RequestDataType.RequestDataType,
                              requests: java.util.ArrayList[_ <: Any], checkJobProperty: Boolean): java.util.ArrayList[T] = {

    try {

      val starttime = System.currentTimeMillis()
      var classificationList: List[(String, HashMap[String, Any])] = null
      var poolList: List[String] = null

      if (checkJobProperty) {
        val values = checkJobPropertiesAndUpdateStats(requests, requestDataType)
        classificationList = values._1
        poolList = values._2
      }
      
      val callableResponses = new java.util.ArrayList[Callable[T]]();
      val isIDSet = false;
      val itr = requests.iterator
      val classificationIterator = classificationList.iterator
      while (itr.hasNext()) {
        val key = itr.next()
        val classificationDetail = classificationIterator.next()
        if (!isIDSet) {
          setCallId(key);
        }
        val queryExecutorTask = new QueryExecutor[T](this,
          HttpUtils.getLoginInfo(), key, requestDataType, classificationDetail._2)
        callableResponses.add(queryExecutorTask)
      }
      
      servMultiple[T](callableResponses, requests, requestDataType, checkJobProperty, classificationList.map(x => x._1), poolList)
    } catch {
      case e: TimeoutException =>
        throw new AcumeException(AcumeExceptionConstants.TIMEOUT_EXCEPTION.name);
      case e: Throwable =>
        throw e;
    }
  }
  
  /**
   * Serves only aggregate request. if request type is timeseries this method fails.
   */
  def  servAggregateSingleQuery(queryRequest : QueryRequest, property: HashMap[String, Any] = null) : AggregateResponse = {
    dataService.servAggregate(queryRequest, property)
  }
  
  def servTimeseriesSingleQuery(queryRequest : QueryRequest, property: HashMap[String, Any] = null) : TimeseriesResponse = {
    dataService.servTimeseries(queryRequest, property)
  }
  
  def  servSingleQuery(queryRequest : String, property: HashMap[String, Any] = null) : Serializable = {
    dataService.servRequest(queryRequest, property).asInstanceOf[Serializable]
  }
  
  def  servAggregateMultiple(queryRequests : java.util.ArrayList[QueryRequest], checkJobProperty: Boolean = true) : java.util.ArrayList[AggregateResponse] = {
    servMultiple[AggregateResponse](RequestDataType.Aggregate, queryRequests, checkJobProperty)
  }
  
  def  servAggregateQuery(queryRequest : QueryRequest, checkJobProperty: Boolean = true) : AggregateResponse = {
    servMultiple[AggregateResponse](RequestDataType.SQL, new java.util.ArrayList(List(queryRequest)), checkJobProperty).get(0)
  }
  
  def servTimeseriesMultiple(queryRequests : java.util.ArrayList[QueryRequest], checkJobProperty: Boolean = true) : java.util.ArrayList[TimeseriesResponse] = {
    servMultiple[TimeseriesResponse](RequestDataType.TimeSeries, queryRequests, checkJobProperty)
  }
  
  def servTimeseriesQuery(queryRequest : QueryRequest, checkJobProperty: Boolean = true) : TimeseriesResponse = {
    servMultiple[TimeseriesResponse](RequestDataType.SQL, new java.util.ArrayList(List(queryRequest)), checkJobProperty).get(0)
  }
  
  def  servSqlQuery(queryRequest : String, checkJobProperty: Boolean = true) : Serializable = {
    servMultiple[Serializable](RequestDataType.SQL, new java.util.ArrayList(List(queryRequest)), checkJobProperty).get(0)
  }
  
  def servSqlQueryMultiple(queryRequests : java.util.ArrayList[String], checkJobProperty: Boolean = true) : java.util.ArrayList[Serializable] = {
    servMultiple[Serializable](RequestDataType.SQL, queryRequests, checkJobProperty)
  }
  
  def  servSqlQuery2(queryRequest : String) = {
    dataService.execute(queryRequest)
  }
  
  def searchRequest(searchRequest : SearchRequest) : SearchResponse = {
    dataService.servSearchRequest(searchRequest)
  }
  
  def servExportCSV(dataExportRequest: DataExportRequest) : Response = {
    try {
        val dataExporter : IDataExporter = DataExporterUtil.getExporterInstance(dataExportRequest.getFileType());
        val dataExporterResponse : DataExportResponse = dataExporter.exportToFile(dataExportRequest);
        val rBuild : ResponseBuilder = Response.ok();
            
        var fileName : String = null
        
        if(dataExporterResponse.getFileType().equals(AcumeConstants.ZIP)) {
          rBuild.`type`("application/zip")
          fileName = CSVDataExporter.getFileName(dataExportRequest.getFileName(), FILE_TYPE.ZIP);
        } else {
          rBuild.`type`("text/csv");
          fileName = CSVDataExporter.getFileName(dataExportRequest.getFileName(), FILE_TYPE.RESULTS);
        }
        
        rBuild.header("Content-Disposition", "attachment;filename=\"" + fileName + "\"");
        rBuild.entity(dataExporterResponse.getInputStream())
        rBuild.build();
    } catch {
      case e: Exception => {
        throw new BadRequestException(HttpError.NOT_ACCEPTABLE, AcumeExceptionConstants.UNABLE_TO_SERVE.toString, 
          e.getMessage, ExceptionUtils.getStackTrace(e))
      }
    }
  }
  
}


object AcumeService {
  
  val acumeService = ConfigFactory.getInstance().getBean(classOf[AcumeService])
  
  val dataService = ConfigFactory.getInstance().getBean(classOf[DataService])
}