package com.guavus.acume.rest.api

import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.authenticate.Authentication
import com.guavus.rubix.query.remote.flex.QueryRequest
import javax.ws.rs.Consumes
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.DefaultValue
import javax.xml.bind.annotation.XmlRootElement
import javax.ws.rs.POST
import com.guavus.rubix.query.remote.flex.SearchRequest
import com.guavus.acume.cache.workflow.AcumeCacheResponse
import com.guavus.acume.core.AcumeContextTraitUtil
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.core.query.DataExportRequest
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.rubix.user.management.utils.UserManagementUtils
import java.io.Serializable
import com.guavus.acume.core.scheduler.Controller
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.rubix.user.management.vo.CurrentSessionInfo
import org.apache.shiro.subject.Subject
import org.apache.shiro.session.Session
import org.apache.shiro.SecurityUtils
import com.guavus.rubix.user.management.exceptions.HttpUMException
import com.guavus.rubix.user.management.vo.LoginRequest
import com.guavus.rubix.user.management.vo.LoginResponse
import com.guavus.rubix.user.management.ui.RoleVO
import com.guavus.acume.core.PSUserService
import com.guavus.rubix.query.remote.flex.TimeZoneInfo
import com.guavus.rubix.query.remote.flex.ZoneInfoRequest
import com.guavus.rubix.user.management.vo.ValidateSessionRequest
import com.guavus.acume.core.scheduler.AcumeCacheAvailabilityPolicy
import scala.collection.mutable.HashMap
import com.guavus.rubix.cache.Interval
import com.guavus.acume.core.scheduler.UnionizedCacheAvailabilityPolicy
import com.guavus.rubix.query.remote.flex.LoginParameterRequest

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
    
    Authentication.authenticate(userinfo, user, password)
    dataExportRequest.setRequestDataType("Aggregate")
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest).asInstanceOf[Serializable]
  }
  
  @POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("exporttimeseries")
  def exportTimeseriesData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String): Serializable = {
    
    Authentication.authenticate(userinfo, user, password)
    dataExportRequest.setRequestDataType("Timeseries")
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest).asInstanceOf[Serializable]
  }
  
  @POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("exportsqlaggregate")
  def exportSqlAggregateData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String): Serializable = {
    
    Authentication.authenticate(userinfo, user, password)
    dataExportRequest.setRequestDataType("Aggregate")
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
  
	@POST
	@Produces(Array("application/json"))
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
	def raeServSqlQueryOnDataSource(query : String, @DefaultValue("cache") @QueryParam("datasource") datasource : String, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
		val startTime = System.currentTimeMillis();
		Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
		AcumeService.acumeService.servSqlQuery(query).asInstanceOf[Serializable]
	}
	
	@POST
	@Path("dataAvailability")
	def getDataAvailability( @QueryParam(value = "super") userinfo : String, @QueryParam("user") user : String, @QueryParam("password") password : String, 
      @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : java.util.HashMap[String, java.util.ArrayList[Long]] = {
	  Authentication.authenticate(userinfo, user, password)

    val controller = ConfigFactory.getInstance.getBean(classOf[Controller])
	  val map = new java.util.HashMap[String, java.util.ArrayList[Long]]()
	  val list = new java.util.ArrayList[Long]()
	  list.add(controller.getFirstBinPersistedTime(ConfConstants.acumecorebinsource))
	  list.add(controller.getLastBinPersistedTime(ConfConstants.acumecorebinsource))
	  
	  //placeholder bin source
	  map.put("abcd", list)
	  return map;
	}
	
	@POST
	@Path("validateQuery")
	def isValidQuery(sql : String, @DefaultValue("cache") @QueryParam("datasource") datasource : String, @QueryParam(value = "super") userinfo : String,
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
  
  @POST
  @Path("validateSession")
  def getValidSession(validateSessionRequest : ValidateSessionRequest) : CurrentSessionInfo = {
    try{
     val currentsessionInfo : CurrentSessionInfo = UserManagementUtils.getIWebUMService().validateSession(validateSessionRequest);
     for (roles:RoleVO <- currentsessionInfo.getRoles){
      roles.setGroups(null)
      roles.setUsers(null)
    }
     currentsessionInfo
    } catch {
      case ex : HttpUMException =>{
        throw ex
      } //logger.warn("Invalid session. Trying to authenticate through rubix db")
        
    }
  }
  
  @POST
  @Path("login")
  def getLoginResponse(loginParameterRequest : LoginParameterRequest) : LoginResponse = {
    val loginRequest : LoginRequest = new LoginRequest()
    loginRequest.setUserName(loginParameterRequest.getUserName())
    loginRequest.setPassword(loginParameterRequest.getPassword())
    loginRequest.setAuthToken(null)
    val response : LoginResponse = UserManagementUtils.getIWebUMService().login(loginRequest)
    for (roles:RoleVO <- response.getCurrentSessionInfo.getRoles){
      roles.setGroups(null)
      roles.setUsers(null)
    }
    response
  }
  
  @GET
  @Path("getTimeRange")
  def getTimeRange(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String) : Array[Long] = {
    Authentication.authenticate(userinfo, user, password)
      new PSUserService().getTimeRange()
  }
  
  @POST
  @Path("zoneInfo")
  def getZoneInfo(zoneInfo : ZoneInfoRequest,@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String) : java.util.List[TimeZoneInfo] = {
    Authentication.authenticate(userinfo, user, password)
    new PSUserService().getZoneInfo(zoneInfo.getIdList(), zoneInfo.getZoneInfoParams())
  }
  
  @POST
  @Path("instaAvailability")
  def getInstaAvailabilty(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("binSource") binSource : String) : Map[Long, (Long, Long)] = {
    Authentication.authenticate(userinfo, user, password)
    new PSUserService().getInstaTimeInterval(binSource)
  }
  
  @POST
  @Path("acumeAvailability")
  def getAcumeAvailabilty(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String) : HashMap[String, HashMap[Long, Interval]] = {
    Authentication.authenticate(userinfo, user, password)
    new AcumeCacheAvailabilityPolicy().getCacheAvalabilityMap
  }
  
  @POST
  @Path("unionizedCache")
  def getUnionizedCacheAvailability(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String) : HashMap[String, HashMap[Long, Interval]] = {
    Authentication.authenticate(userinfo, user, password)
    new UnionizedCacheAvailabilityPolicy().getCacheAvalabilityMap
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

}
