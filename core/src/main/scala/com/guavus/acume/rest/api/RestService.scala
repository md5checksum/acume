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

@Path("/" + "queryresponse")
/**
 * Expose Acume all rest apis.
 */
class RestService {
	

	@GET
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("aggregate")
	def servAggregate(@QueryParam("query") query : QueryRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) = {
	  servQuery(query, userinfo, user, password, getAdditionalInfo, true)
	}
	
	
	@GET
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("timeseries")
	def servTimeseries(@QueryParam("query") query : QueryRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) {
	  servQuery(query, userinfo, user, password, getAdditionalInfo, false)
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
		  AcumeService.acumeService.servAggregateQuery(query)
		}
		return null
	}
	

}
