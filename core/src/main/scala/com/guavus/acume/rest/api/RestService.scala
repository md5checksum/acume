package com.guavus.acume.rest.api

import javax.ws.rs.Path
import javax.ws.rs.core.UriInfo
import javax.ws.rs.core.Context
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import com.guavus.rubix.RubixWebService.ValidationResponse
import com.guavus.rubix.RubixWebService.RequestValidationParams
import com.guavus.rubix.query.QueryRequest
import com.guavus.acume.core.authenticate.Authentication

@Path("/" + "query")
/**
 * Expose Acume all rest apis.
 */
abstract class RestService {
	@Context
	val info : UriInfo
	@Context
	val request : HttpServletRequest
	@Context
	val response : HttpServletResponse
	
	

	/**
	 * Takes rubix like query as input with additional params and return response. This handles timeseries as well as aggregate queries
	 */
	def servQuery(query : QueryRequest, userinfo : String,
			user : String, password : String, getAdditionalInfo : Boolean, isAggregate : Boolean) : Serializable = {
		val startTime = System.currentTimeMillis();
		Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
		
		return null
	}
	

}
