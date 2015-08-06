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
import java.util.concurrent.TimeUnit

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

    def runWithTimeout[T](callable: Callable[java.util.ArrayList[T]]): java.util.ArrayList[T] = {
      val response = QueryExecutorThreads.getPoolMultiple.submit(callable)
      try {
        response.get(dataService.acumeContext.acumeConf.getLong(ConfConstants.queryTimeOut).getOrElse(30l), TimeUnit.SECONDS)
      } catch {
        case e : Exception => {
          if(!response.isDone()) {
            response.cancel(true)
          }
          throw e;
        }
      }
    }
    
    def run[T](callable: Callable[java.util.ArrayList[T]]): java.util.ArrayList[T] = {
        callable.call()
    }

    val callable = new Callable[java.util.ArrayList[T]]() {
      def call() = {

        val futureResponses = new java.util.ArrayList[Future[T]]();
        val threadPool = QueryExecutorThreads.getPool();
        val isIDSet = false;

        callableResponses foreach (callableResponse => {
          futureResponses.add(threadPool.submit(callableResponse))
        })

        val responses = new java.util.ArrayList[T]()
        var classificationIterator: Iterator[String] = null
        var poolIterator: Iterator[String] = null
        if (checkJobProperty) {
          classificationIterator = classificationList.iterator
          poolIterator = poolList.iterator
        }

        try {
          for (futureResponse <- futureResponses) {
            try {
              responses.add(futureResponse.get())
              if (checkJobProperty && poolIterator.hasNext)
                dataService.queryPoolPolicy.updateStats(poolIterator.next(), classificationIterator.next(), dataService.poolStats, dataService.classificationStats, starttime, System.currentTimeMillis())
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
          for (futureResponse <- futureResponses) {
            if (!futureResponse.isDone()) {
              futureResponse.cancel(true)
            }
          }
          if (checkJobProperty) {
            while (poolIterator.hasNext) {
              dataService.queryPoolPolicy.updateStats(poolIterator.next(), classificationIterator.next(), dataService.poolStats, dataService.classificationStats, starttime, System.currentTimeMillis())
            }
            dataService.queryPoolPolicy.updateFinalStats(poolList.iterator.next(), classificationList.iterator.next(), dataService.poolStats, dataService.classificationStats, starttime, System.currentTimeMillis())
          }
        }
        responses
      }
    }
    if(dataService.acumeContext.acumeConf.getBoolean(ConfConstants.schedulerQuery).getOrElse(false)) {
      run(callable)
    } else {
      runWithTimeout[T](callable)
    }
  }
  
  def checkJobPropertiesAndUpdateStats(requests: java.util.ArrayList[_ <: Any], requestDataType: RequestDataType.RequestDataType): (List[(String, HashMap[String, Any])], List[String]) = {
    var poolname: String = null
    var classificationname: String = null
    this.synchronized {
      var classificationandpool = dataService.checkJobLevelProperties(requests, requestDataType)
      dataService.queryPoolPolicy.updateInitialStats(classificationandpool._2, classificationandpool._1.map(x => x._1), dataService.poolStats, dataService.classificationStats)
      classificationandpool
    }
  }
  
  
  //Developer API for not calling checkJobproperties and directly calling the QueryExecutor
  def servMultiple[T](requestDataType: RequestDataType.RequestDataType,
                              requests: java.util.ArrayList[_ <: Any], datasourceName: String, checkJobProperty: Boolean = true): java.util.ArrayList[T] = {

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
      val classificationIterator = if (classificationList != null) classificationList.iterator else null
      val poolIterator = if (poolList != null) poolList.iterator else null
      while (itr.hasNext()) {
        val key = itr.next()
        var classificationDetail: (String, HashMap[String, Any]) = (null, null)
        var poolName: String = null
        if (poolIterator != null && poolIterator.hasNext) {
          poolName = poolIterator.next()
        }
        
        if (classificationIterator != null && classificationIterator.hasNext) {
          classificationDetail = classificationIterator.next()
          classificationDetail._2.put("spark.scheduler.pool", poolName)
        }
        
        if (!isIDSet) {
          setCallId(key);
        }
        val queryExecutorTask = new QueryExecutor[T](this,
          HttpUtils.getLoginInfo(), key, requestDataType, datasourceName, classificationDetail._2)
        callableResponses.add(queryExecutorTask)
      }
      
      servMultiple[T](callableResponses, requests, requestDataType, checkJobProperty, if (classificationList != null) classificationList.map(x => x._1) else null, poolList)
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
  def  servAggregateSingleQuery(queryRequest : QueryRequest, datasourceName: String, property: HashMap[String, Any] = null) : AggregateResponse = {
    val datasourceName = AcumeContextTraitUtil.dsInterpreterPolicy.interpretDsName(queryRequest.toSql(""))
    dataService.servAggregate(queryRequest, property)
  }
  
  def servTimeseriesSingleQuery(queryRequest : QueryRequest, datasourceName: String, property: HashMap[String, Any] = null) : TimeseriesResponse = {
    val datasourceName = AcumeContextTraitUtil.dsInterpreterPolicy.interpretDsName(queryRequest.toSql(""))
    dataService.servTimeseries(queryRequest, property)
  }
  
  def  servSingleQuery(queryRequest : String, datasourceName: String, property: HashMap[String, Any] = null) : Serializable = {
    val datasourceName = AcumeContextTraitUtil.dsInterpreterPolicy.interpretDsName(queryRequest)
    dataService.servRequest(queryRequest, property).asInstanceOf[Serializable]
  }
  
  def  servAggregateMultiple(queryRequests : java.util.ArrayList[QueryRequest], datasourceName: String) : java.util.ArrayList[AggregateResponse] = {
    servMultiple[AggregateResponse](RequestDataType.Aggregate, queryRequests, datasourceName)
  }
  
  def  servAggregateQuery(queryRequest : QueryRequest, datasourceName: String) : AggregateResponse = {
    servMultiple[AggregateResponse](RequestDataType.Aggregate, new java.util.ArrayList(List(queryRequest)), datasourceName).get(0)
  }
  
  def servTimeseriesMultiple(queryRequests : java.util.ArrayList[QueryRequest], datasourceName: String) : java.util.ArrayList[TimeseriesResponse] = {
    servMultiple[TimeseriesResponse](RequestDataType.TimeSeries, queryRequests, datasourceName)
  }
  
  def servTimeseriesQuery(queryRequest : QueryRequest, datasourceName: String) : TimeseriesResponse = {
    servMultiple[TimeseriesResponse](RequestDataType.TimeSeries, new java.util.ArrayList(List(queryRequest)), datasourceName).get(0)
  }
  
  def  servSqlQuery(queryRequest : String, datasourceName: String) : Serializable = {
    servMultiple[Serializable](RequestDataType.SQL, new java.util.ArrayList(List(queryRequest)), datasourceName).get(0)
  }
  
  def servSqlQueryMultiple(queryRequests : java.util.ArrayList[String], datasourceName: String) : java.util.ArrayList[Serializable] = {
    servMultiple[Serializable](RequestDataType.SQL, queryRequests, datasourceName)
  }
  
  def  servSqlQuery2(queryRequest : String, datasourceName: String) = {
    dataService.execute(queryRequest)
  }
  
  def searchRequest(searchRequest : SearchRequest, datasourceName: String) : SearchResponse = {
    dataService.servSearchRequest(searchRequest)
  }
  
  def servExportCSV(dataExportRequest: DataExportRequest, datasourceName: String) : Response = {
    try {
        val dataExporter : IDataExporter = DataExporterUtil.getExporterInstance(dataExportRequest.getFileType());
        val dataExporterResponse : DataExportResponse = dataExporter.exportToFile(dataExportRequest, datasourceName);
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