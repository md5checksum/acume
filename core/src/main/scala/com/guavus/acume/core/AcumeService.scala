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

/**
 * Main service of acume which serves the request from UI and rest services. It checks if the response is present in RR cache otherwise fire the query on OLAP cache.
 */
class AcumeService(dataService: DataService) {
  
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
    new java.util.ArrayList(queryRequests.map(servAggregateQuery(_)))
  }
  
  /**
   * Serves only aggregate request. if request type is timeseries this method fails.
   */
  def  servAggregateQuery(queryRequest : QueryRequest) : AggregateResponse = {
    dataService.servAggregate(queryRequest)
  }
  
  def servTimeseriesMultiple(queryRequests : java.util.ArrayList[QueryRequest]) : java.util.ArrayList[TimeseriesResponse] = {
    new java.util.ArrayList(queryRequests.map(servTimeseriesQuery(_)))
  }
  
  def servTimeseriesQuery(queryRequest : QueryRequest) : TimeseriesResponse = {
    dataService.servTimeseries(queryRequest)
  }
  
  def  servSqlQuery(queryRequest : String) : Serializable = {
    dataService.servRequest(queryRequest).asInstanceOf[Serializable]
  }
  
  def  servSqlQuery2(queryRequest : String) = {
    dataService.execute(queryRequest)
  }
  
  def searchRequest(searchRequest : SearchRequest) : SearchResponse = {
    dataService.servSearchRequest(searchRequest)
  }
}


object AcumeService {
  
  val acumeService = ConfigFactory.getInstance().getBean(classOf[AcumeService])
  
  val dataService = ConfigFactory.getInstance().getBean(classOf[DataService])
}