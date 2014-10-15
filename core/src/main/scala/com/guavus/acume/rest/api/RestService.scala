package com.guavus.acume.rest.api

import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.authenticate.Authentication
import com.guavus.acume.rest.beans.QueryRequest
import javax.ws.rs.Consumes
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.xml.bind.annotation.XmlRootElement
import javax.ws.rs.POST
import com.guavus.acume.rest.beans.SearchRequest

@Path("/" + "queryresponse")
/**
 * Expose Acume all rest apis.
 */
class RestService {
	

	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("aggregate")
	def servAggregate(query : QueryRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) = {
	  servQuery(query, userinfo, user, password, getAdditionalInfo, true)
	}
	
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("timeseries")
	def servTimeseries(query : QueryRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) {
	  servQuery(query, userinfo, user, password, getAdditionalInfo, false)
	}
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("timeseries")
	def servSearchQuery(query : SearchRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) {
	  Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
	  AcumeService.acumeService.searchRequest(query).asInstanceOf[Serializable]
	}
  
	/**
	 * Takes rubix like query as input with additional params and return response. This handles timeseries as well as aggregate queries
	 */
	def servQuery(query : QueryRequest, userinfo : String,
			user : String, password : String, getAdditionalInfo : Boolean, isAggregate : Boolean) : Serializable = {
		val startTime = System.currentTimeMillis();
		Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
		if(isAggregate) {
		  AcumeService.acumeService.servAggregateQuery(query).asInstanceOf[Serializable]
		} else {
			AcumeService.acumeService.servTimeseriesQuery(query).asInstanceOf[Serializable]
		}
	}
	
	@POST
	@Path("sql")
	def servSqlQuery(query : String,  @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
		val startTime = System.currentTimeMillis();
		Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
		  AcumeService.acumeService.servSqlQuery(query).asInstanceOf[Serializable]
	}
	

}
