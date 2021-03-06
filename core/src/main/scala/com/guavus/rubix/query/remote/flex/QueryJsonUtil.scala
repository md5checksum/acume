package com.guavus.rubix.query.remote.flex

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.util.Arrays
import com.google.common.collect.Lists
import java.util.Collections
import com.guavus.acume.core.query.DataExportRequest
import com.guavus.acume.core.query.DataExportResponse
import com.guavus.rubix.user.management.vo.LoginRequest
import com.guavus.rubix.user.management.vo.ValidateSessionRequest
import com.guavus.rubix.user.management.vo.LoginResponse
import com.guavus.rubix.user.management.vo.LogoutRequest
import com.guavus.acume.rest.api.MessageBodyReaderJSON
import com.guavus.rubix.search.SearchRequest
import com.guavus.rubix.search.SearchCriterion
import com.guavus.rubix.search.SearchCriteria

object QueryJsonUtil {

  private var gson: Gson = new GsonBuilder().create()
  
  def dataExportResponseToJson(request : DataExportResponse) : String = gson.toJson(request)
  
  def dataExportRequestToJson(request : DataExportRequest) : String = gson.toJson(request)

  def fromJsonToExportRequest(json: String): DataExportRequest = {
    gson.fromJson(json, classOf[DataExportRequest])
  }

  def queryRequestToJson(request: QueryRequest): String = gson.toJson(request)

  def fromJsonToQueryRequest(json: String): QueryRequest = {
    gson.fromJson(json, classOf[QueryRequest])
  }
  
  def fromJsonToQueryRequests(json: String): Array[QueryRequest] = {
    gson.fromJson(json, classOf[Array[QueryRequest]])
  }
  
  
  def fromSearchRequestToJson(request : SearchRequest) = {
	gson.toJson(request)
  }
  
  def aggregateResponseToJson(response: AggregateResponse): String = gson.toJson(response)

  def aggregateResponsesToJson(response: java.util.ArrayList[AggregateResponse]): String = gson.toJson(response)
  
  def timeseriesResponsesToJson(response: java.util.ArrayList[TimeseriesResponse]): String = gson.toJson(response)
  
  def timeseriesResponseToJson(response: TimeseriesResponse): String = gson.toJson(response)

  def fromJsonToSearchRequest(json: String): SearchRequest = {
    gson.fromJson(json, classOf[SearchRequest])
  }
  
  def main(args: Array[String]) {
	val search = new SearchRequest() 
	search.setResponseDimensions(Lists.newArrayList("a", "b", "c"))
	search.setStartTime(1212)
	search.setEndTime(1212)
	search.setLimit(100)
	val sc = new SearchCriteria()
	search.setSearchCriteria(sc)
	val scn = new SearchCriterion()
	scn.setDimensionName("a")
	scn.setValue("1")
	sc.addCriteria(scn)
	scn.setOperator("GREATER_THAN")
	val s = MessageBodyReaderJSON.gson.fromJson(fromSearchRequestToJson(search), classOf[SearchRequest]);
	println(s.toSql)
	println("pankaj");
  }
  
  def fromJsonToZoneInfoRequest(json: String): ZoneInfoRequest = {
    gson.fromJson(json, classOf[ZoneInfoRequest])
  }
  
  def zoneInfoRequestToJson(response: ZoneInfoRequest): String = gson.toJson(response)
   
  def fromJsonToValidateSessionRequest(json: String): ValidateSessionRequest = {
    gson.fromJson(json, classOf[ValidateSessionRequest])
  }
  
   def fromJsonToLoginParameterRequest(json: String): LoginParameterRequest = {
    gson.fromJson(json, classOf[LoginParameterRequest])
  }
  
  def loginParameterRequestToJson(response: LoginParameterRequest): String = gson.toJson(response)
  
  def fromJsonToLogoutRequest(json: String): LogoutRequest = {
    gson.fromJson(json, classOf[LogoutRequest])
  }
  
/*
Original Java:
package com.guavus.acume.rest.beans;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guavus.rubix.search.SearchRequest;

public class QueryJsonUtil {
	
	private static Gson gson  = new GsonBuilder().create()  ;
	
	public static String queryRequestToJson(QueryRequest request ){
		return gson.toJson(request);
	}
	

	public static QueryRequest fromJsonToQueryRequest(String json){
		return gson.fromJson(json, QueryRequest.class);
	}
	
	public static String aggregateResponseToJson(AggregateResponse response){
		return gson.toJson(response);
	}
	
	public static String timeseriesResponseToJson(TimeseriesResponse response){
		return gson.toJson(response);
	}
	
	public static SearchRequest fromJsonToSearchRequest(String json){
		return gson.fromJson(json, SearchRequest.class);
	}	
	
}

*/
}