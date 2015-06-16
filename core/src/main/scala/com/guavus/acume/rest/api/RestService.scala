package com.guavus.acume.rest.api

import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.authenticate.Authentication
import com.guavus.rubix.query.remote.flex.QueryRequest
import javax.ws.rs.Consumes
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.xml.bind.annotation.XmlRootElement
import javax.ws.rs.POST
import com.guavus.rubix.query.remote.flex.SearchRequest
import com.guavus.acume.cache.workflow.AcumeCacheResponse
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.core.query.DataExportRequest
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.rubix.user.management.utils.UserManagementUtils
import java.io.Serializable

@Path("/" + "queryresponse")
/**
 * Expose Acume all rest apis.
 */
class RestService {
  
  @POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("exportaggregate")
  def exportAggregateData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String): Serializable = {
    
   UserManagementUtils.getIWebUMService().validateSession(null);
    dataExportRequest.setRequestDataType(RequestType.Aggregate)
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest).asInstanceOf[Serializable]
  }
  
  @POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("exporttimeseries")
  def exportTimeseriesData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String): Serializable = {
    
    UserManagementUtils.getIWebUMService().validateSession(null);
    dataExportRequest.setRequestDataType(RequestType.Timeseries)
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest).asInstanceOf[Serializable]
  }
  
  @POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("exportsqlaggregate")
  def exportSqlAggregateData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String): Serializable = {
    
    UserManagementUtils.getIWebUMService().validateSession(null);
    dataExportRequest.setRequestDataType(RequestType.Aggregate)
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest).asInstanceOf[Serializable]
  }

	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("aggregate")
	def servAggregate(query : QueryRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
	  servQuery(query, userinfo, user, password, getAdditionalInfo, true)
	}
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("timeseries")
	def servTimeseries(query : QueryRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
	  servQuery(query, userinfo, user, password, getAdditionalInfo, false)
	}
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("aggregateMultiple")
	def servAggregateMultiple(query : java.util.ArrayList[QueryRequest], @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
	  servMultiple(query, userinfo, user, password, getAdditionalInfo, true)
	}
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("timeseriesMultiple")
	def servTimeseriesMultiple(query : java.util.ArrayList[QueryRequest], @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
	  servMultiple(query, userinfo, user, password, getAdditionalInfo, false)
	}
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("search")
	def servSearchQuery(query : SearchRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
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
	
	def servMultiple(query : java.util.ArrayList[QueryRequest], userinfo : String,
			user : String, password : String, getAdditionalInfo : Boolean, isAggregate : Boolean) : Serializable = {
		val startTime = System.currentTimeMillis();
		Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
		if(isAggregate) {
		  AcumeService.acumeService.servAggregateMultiple(query).asInstanceOf[Serializable]
		} else {
			AcumeService.acumeService.servTimeseriesMultiple(query).asInstanceOf[Serializable]
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
	
	@POST
	@Path("raeSql")
	def raeServSqlQueryOnDataSource(query : String,  @QueryParam(value = "dataSource") dataSource : String, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
		val startTime = System.currentTimeMillis();
		Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
		AcumeService.acumeService.servSqlQuery(query).asInstanceOf[Serializable]
	}
	
	@POST
	@Path("dataAvailability")
	def getDataAvailability( @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : java.util.HashMap[String, java.util.ArrayList[Long]] = {
	  Authentication.authenticate(userinfo, user, password)

	  val map = new java.util.HashMap[String, java.util.ArrayList[Long]]()
	  val list = new java.util.ArrayList[Long]()
	  list.add(AcumeContextTrait.acumeContext.get.acumeContext.cacheConf.getLong("acume.cache.delete.firstbinpersistedtime"))
	  list.add(AcumeContextTrait.acumeContext.get.acumeContext.cacheConf.getLong("acume.cache.delete.lastbinpersistedtime"))
	  
	  //placeholder bin source
	  map.put("abcd", list)
	  return map;
	}
	
	@POST
	@Path("validateQuery")
	def isValidQuery(sql : String, @QueryParam(value = "dataSource") dataSource : String, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : java.lang.Boolean = {
	  Authentication.authenticate(userinfo, user, password)
	  true
	}
	
	@POST
	@Path("validDataSources")
	def getValidDataSources(@QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : java.util.ArrayList[String] = {
	  Authentication.authenticate(userinfo, user, password)
	  new java.util.ArrayList()
	}

}
