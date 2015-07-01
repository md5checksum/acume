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
  
  /**
   * Serves only aggregate request. if request type is timeseries this method fails.
   */
  def  servAggregateMultiple(queryRequests : java.util.ArrayList[QueryRequest]) : java.util.ArrayList[AggregateResponse] = {
    servMultiple[AggregateResponse](RequestDataType.Aggregate, queryRequests)
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
  
  private def servMultipleCallables[T](callableResponses: java.util.ArrayList[Callable[T]], threadPool: ExecutorService): java.util.ArrayList[T] = {
    val futureResponses = new java.util.ArrayList[Future[T]]();
    val isIDSet = false;
    
    callableResponses foreach (callableResponse => {
      futureResponses.add(threadPool.submit(callableResponse))
    })
    
    val responses = new java.util.ArrayList[T]()
      for (futureResponse <- futureResponses) {
        try {
          responses.add(futureResponse.get())
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
    responses
  }

  private def servMultiple[T](requestDataType: RequestDataType.RequestDataType,
                              requests: java.util.ArrayList[_ <: Any]): java.util.ArrayList[T] = {

    var sql = requestDataType match {
      case RequestDataType.Aggregate  => requests.get(0).asInstanceOf[QueryRequest].toSql("")
      case RequestDataType.TimeSeries => requests.get(0).asInstanceOf[QueryRequest].toSql("ts,")
      case RequestDataType.SQL        => requests.get(0).asInstanceOf[String]
      case _                          => throw new IllegalArgumentException("QueryExecutor does not support request type: " + requestDataType)
    }

    val starttime = System.currentTimeMillis()
    var poolname: String = null
    var classificationname: String = null
    var poolStatAttribute: StatAttributes = null
    var classificationStatAttribute: StatAttributes = null
    this.synchronized {
    
    var classification_pool = dataService.checkJobLevelProperties(sql)
    poolname = classification_pool._2
    classificationname = classification_pool._1

    dataService.updateInitialStats(poolname, classificationname)
    }
    
    val responses = scala.collection.mutable.HashMap[Any, T]()

    try {
      val callableResponses = new java.util.ArrayList[Callable[T]]();
      val isIDSet = false;
      val threadPool = QueryExecutorThreads.getPool();
      val itr = requests.iterator
      while (itr.hasNext()) {
        val key = itr.next()
        if (!isIDSet) {
          setCallId(key);
        }
        val queryExecutorTask = new QueryExecutor[T](this,
          HttpUtils.getLoginInfo(), key, requestDataType)
        callableResponses.add(queryExecutorTask)
      }
      
      servMultipleCallables[T](callableResponses, threadPool)

    } finally {
      if (classificationname != null && poolname != null) {
        poolStatAttribute = dataService.poolStats.getStatsForPool(poolname)
        classificationStatAttribute = dataService.classificationStats.getStatsForClassification(classificationname)
        println("poolname delete : ", poolStatAttribute.currentRunningQries.get)
        println("classificationStatAttribute delete : ", classificationStatAttribute.currentRunningQries.get)
        dataService.updateFinalStats(poolname, classificationname, poolStatAttribute, classificationStatAttribute, starttime, System.currentTimeMillis())
      }
    }
  }

  
  /**
   * Serves only aggregate request. if request type is timeseries this method fails.
   */
  def  servAggregateQuery(queryRequest : QueryRequest) : AggregateResponse = {
    dataService.servAggregate(queryRequest)
  }
  
  def servTimeseriesMultiple(queryRequests : java.util.ArrayList[QueryRequest]) : java.util.ArrayList[TimeseriesResponse] = {
    servMultiple[TimeseriesResponse](RequestDataType.TimeSeries, queryRequests)
  }
  
  def servTimeseriesQuery(queryRequest : QueryRequest) : TimeseriesResponse = {
    dataService.servTimeseries(queryRequest)
  }
  
  def  servSqlQuery(queryRequest : String) : Serializable = {
    dataService.servRequest(queryRequest).asInstanceOf[Serializable]
  }
  
  def servSqlQueryMultiple(queryRequests : java.util.ArrayList[String]) : java.util.ArrayList[Serializable] = {
    servMultiple[Serializable](RequestDataType.SQL, queryRequests)
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