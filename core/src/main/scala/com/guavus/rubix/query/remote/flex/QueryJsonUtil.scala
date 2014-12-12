package com.guavus.rubix.query.remote.flex

import com.google.gson.Gson
import com.google.gson.GsonBuilder

object QueryJsonUtil {

  private var gson: Gson = new GsonBuilder().create()

  def queryRequestToJson(request: QueryRequest): String = gson.toJson(request)

  def fromJsonToQueryRequest(json: String): QueryRequest = {
    gson.fromJson(json, classOf[QueryRequest])
  }

  def aggregateResponseToJson(response: AggregateResponse): String = gson.toJson(response)

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