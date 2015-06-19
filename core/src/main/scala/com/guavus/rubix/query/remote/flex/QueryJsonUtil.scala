package com.guavus.rubix.query.remote.flex

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import java.util.Arrays
import com.google.common.collect.Lists
import java.util.Collections
import com.guavus.acume.core.query.DataExportRequest
import com.guavus.acume.core.query.DataExportResponse


object QueryJsonUtil {

  private var gson: Gson = new GsonBuilder().create()
  
  def dataExportRequestToJson(request : DataExportRequest) : String = gson.toJson(request)

  def fromJsonToExportResponse(json: String): DataExportResponse = {
    gson.fromJson(json, classOf[DataExportResponse])
  }

  def queryRequestToJson(request: QueryRequest): String = gson.toJson(request)

  def fromJsonToQueryRequest(json: String): QueryRequest = {
    gson.fromJson(json, classOf[QueryRequest])
  }
  
  def fromJsonToQueryRequests(json: String): java.util.ArrayList[QueryRequest] = {
    val array = gson.fromJson(json, classOf[Array[QueryRequest]])
    val response = new java.util.ArrayList[QueryRequest](array.length)
    for(request <- array) {
      response.add(request)
    }
    response
  }
  

  def aggregateResponseToJson(response: AggregateResponse): String = gson.toJson(response)

  def aggregateResponsesToJson(response: java.util.ArrayList[AggregateResponse]): String = gson.toJson(response)
  
  def timeseriesResponsesToJson(response: java.util.ArrayList[TimeseriesResponse]): String = gson.toJson(response)
  
  def timeseriesResponseToJson(response: TimeseriesResponse): String = gson.toJson(response)

  def fromJsonToSearchRequest(json: String): SearchRequest = {
    gson.fromJson(json, classOf[SearchRequest])
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