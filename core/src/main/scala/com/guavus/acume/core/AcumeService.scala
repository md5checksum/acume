package com.guavus.acume.core

import com.guavus.acume.rest.beans.QueryRequest
import com.guavus.acume.core.AcumeService._
import com.guavus.acume.rest.beans.AggregateResponse
import com.guavus.acume.rest.beans.TimeseriesResponse
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.rest.beans.SearchRequest
import com.guavus.acume.rest.beans.SearchResponse

/**
 * Main service of acume which serves the request from UI and rest services. It checks if the response is present in RR cache otherwise fire the query on OLAP cache.
 */
class AcumeService(dataService: DataService) {
  
  /**
   * Any query request can be fired and result of appropriate type is returned, If query is of type aggregate it returns aggregate response else timeseries response.
   */
  def servQuery(queryRequest : QueryRequest) {
    // call the query builder and convert query in standard sql-92 format. After that fires this query on to OLAP cache and materialize the RDD and populate the reponse.
  }
  
  /**
   * Serves only aggregate request. if request type is timeseries this method fails.
   */
  def  servAggregateQuery(queryRequest : QueryRequest) : AggregateResponse = {
    dataService.servAggregate(queryRequest)
  }
  
  def servTimeseriesQuery(queryRequest : QueryRequest) : TimeseriesResponse = {
    dataService.servTimeseries(queryRequest)
  }
  
  def  servSqlQuery(queryRequest : String) : Serializable = {
    dataService.servRequest(queryRequest).asInstanceOf[Serializable]
  }
  
  def searchRequest(searchRequest : SearchRequest) : SearchResponse = {
    dataService.servSearchRequest(searchRequest)
  }
}


object AcumeService {
  
  val acumeService = ConfigFactory.getInstance().getBean(classOf[AcumeService])
  
  val dataService = ConfigFactory.getInstance().getBean(classOf[DataService])
}