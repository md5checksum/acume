package com.guavus.acume.rest.api

import java.io.Serializable
import java.util.ArrayList
import scala.collection.JavaConversions
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.PSUserService
import com.guavus.acume.core.authenticate.Authentication
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.query.DataExportRequest
import com.guavus.acume.core.scheduler.Controller
import com.guavus.acume.core.scheduler.ICacheAvalabilityUpdatePolicy
import com.guavus.acume.workflow.RequestDataType
import com.guavus.rubix.cache.Interval
import com.guavus.rubix.query.remote.flex.LoginParameterRequest
import com.guavus.rubix.query.remote.flex.QueryRequest
import com.guavus.rubix.query.remote.flex.StartEndResponse
import com.guavus.rubix.query.remote.flex.TimeZoneInfo
import com.guavus.rubix.query.remote.flex.ZoneInfoRequest
import com.guavus.rubix.search.SearchRequest
import com.guavus.rubix.user.management.exceptions.HttpUMException
import com.guavus.rubix.user.management.ui.RoleVO
import com.guavus.rubix.user.management.utils.UserManagementUtils
import com.guavus.rubix.user.management.vo.CurrentSessionInfo
import com.guavus.rubix.user.management.vo.LoginRequest
import com.guavus.rubix.user.management.vo.LoginResponse
import com.guavus.rubix.user.management.vo.LogoutRequest
import com.guavus.rubix.user.management.vo.LogoutResponse
import com.guavus.rubix.user.management.vo.ValidateSessionRequest
import javax.ws.rs.Consumes
import javax.ws.rs.DefaultValue
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.xml.bind.annotation.XmlRootElement
import com.guavus.rubix.user.management.vo.ChangePasswordRequest
import acume.exception.AcumeException
import com.guavus.acume.core.exceptions.AcumeExceptionConstants
import com.guavus.acume.core.exceptions.AcumeExceptionConstants._


@Path("/" + "queryresponse")
/**
 * Expose Acume all rest apis.
 */
class RestService {
  
  @POST
  @Consumes(Array("text/plain,text/html,application/x-www-form-urlencoded,application/json"))
  @Path("exportaggregate")
  def exportAggregateData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String) = {
    
    Authentication.authenticate(userinfo, user, password)
    
    dataExportRequest.setRequestDataType(RequestDataType.Aggregate)
    
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest)
  }
  
  @POST
    @Consumes(Array("text/plain,text/html,application/x-www-form-urlencoded,application/json"))
    @Path("exporttimeseries")
  def exportTimeseriesData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String) = {
    
    Authentication.authenticate(userinfo, user, password)
    dataExportRequest.setRequestDataType(RequestDataType.TimeSeries)
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest)
  }
  
  @POST
    @Consumes(Array("application/json"))
    @Path("exportsqlaggregate")
  def exportSqlAggregateData(dataExportRequest: DataExportRequest, @QueryParam(value = "super") userinfo: String,
      @QueryParam("user") user: String, @QueryParam("password") password: String) = {
    
    Authentication.authenticate(userinfo, user, password)
    dataExportRequest.setRequestDataType(RequestDataType.TimeSeries)
    dataExportRequest.setRubixService(AcumeService.acumeService)
    AcumeService.acumeService.servExportCSV(dataExportRequest)
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
    @Path("aggregatemultiple")
	def servAggregateMultiple(query : Array[QueryRequest], @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
    val queryRequests = new ArrayList[QueryRequest](JavaConversions.asJavaList(query.toList))
	  servMultiple(queryRequests, userinfo, user, password, getAdditionalInfo, true)
	}
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("timeseriesmultiple")
	def servTimeseriesMultiple(query : Array[QueryRequest], @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
    val queryRequests = new ArrayList[QueryRequest](JavaConversions.asJavaList(query.toList))
	  servMultiple(queryRequests, userinfo, user, password, getAdditionalInfo, false)
	}
	
	@POST
    @Consumes(Array("application/json"))
    @Produces(Array("application/json"))
    @Path("search")
	def servSearchQuery(query : SearchRequest, @QueryParam(value = "super") userinfo : String,
			@QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("getAddInfo") getAdditionalInfo : Boolean) : Serializable = {
	  Authentication.authenticate(userinfo, user, password)
		// Submit the request to query builder which will return the actual query to be fired on olap cache. It will also return the type of query it was aggregate/timeseries. After receiving
	  AcumeService.acumeService.search(query).asInstanceOf[Serializable]
	}
  
	@POST
	@Consumes(Array("text/plain"))
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
	@Consumes(Array("text/plain"))
  @Produces(Array("application/json"))
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
	@Consumes(Array("text/plain"))
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
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
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
      }
        
    }
  }
  
  @POST
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
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
  
  @POST
  @Consumes(Array("application/json"))
  @Path("changePassword")
  def changePassword(changePasswordRequest : ChangePasswordRequest, @QueryParam(value = "super") userinfo : String,
	      @QueryParam("user") user : String, @QueryParam("password") password : String)  = {
	 //Authentication.authenticate(userinfo, user, password)
   changePasswordRequest.setCurrentPassword(changePasswordRequest.getCurrentPassword)
   changePasswordRequest.setNewPassword(changePasswordRequest.getNewPassword)
     UserManagementUtils.getIWebUMService().changePassword(changePasswordRequest)
     
  }
  
  
  
  @POST
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("getTimeRange")
  def getTimeRange(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String) : Array[Long] = {
      Authentication.authenticate(userinfo, user, password)
      new PSUserService().getTimeRange()
  }
  
  @POST
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("zoneInfo")
  def getZoneInfo(zoneInfo : ZoneInfoRequest,@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String) : java.util.List[TimeZoneInfo] = {
    Authentication.authenticate(userinfo, user, password)
    new PSUserService().getZoneInfo(zoneInfo.getIdList(), zoneInfo.getZoneInfoParams())
  }
  
  @POST
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("instaAvailability")
  def getInstaAvailabilty(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("binSource") binSource : String) : java.util.Map[Long,StartEndResponse] = {
    Authentication.authenticate(userinfo, user, password)
    val response: Map[Long, (Long, Long)] = new PSUserService().getInstaTimeInterval(binSource)
    val instaResponse: java.util.Map[Long,StartEndResponse] = new java.util.HashMap[Long, StartEndResponse]()
    for ((k: Long, v:(Long,Long)) <- response){
      instaResponse.put(k, new StartEndResponse(v._1,v._2))
    }
    instaResponse
  }
  
  @POST
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("acumeAvailability")
  def getAcumeAvailabilty(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String) : java.util.Map[String, java.util.Map[String, Interval]] = {
    Authentication.authenticate(userinfo, user, password)
    val map : HashMap[String, HashMap[Long, Interval]] = ICacheAvalabilityUpdatePolicy.getICacheAvalabiltyUpdatePolicy.getCacheAvalabilityMap
    
    if(map == null || map.isEmpty)
      throw new AcumeException(AcumeExceptionConstants.NO_DATA_EXCEPTION.name)
    
    mapAsJavaMap(map.map(x => (x._1, mapAsJavaMap(x._2.map(y => (y._1.toString, y._2))))))
  }
  
  @POST
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("acumeAvailabilityBinSource")
  def getAcumeAvailabiltyBinSource(@QueryParam(value = "super") userinfo : String,
      @QueryParam("user") user : String, @QueryParam("password") password : String, @QueryParam("binsource") binSource: String) : java.util.Map[String, Interval] = {
    Authentication.authenticate(userinfo, user, password)
    val map : HashMap[String, HashMap[Long, Interval]] = ICacheAvalabilityUpdatePolicy.getICacheAvalabiltyUpdatePolicy.getCacheAvalabilityMap
    
    if(map == null || map.isEmpty)
      throw new AcumeException(AcumeExceptionConstants.NO_DATA_EXCEPTION.name)
    
    val retVal = mapAsJavaMap(map.map(x => (x._1, mapAsJavaMap(x._2.map(y => (y._1.toString, y._2)))))).get(binSource)
    if (retVal == null || retVal.isEmpty)
      throw new AcumeException(AcumeExceptionConstants.NO_DATA_EXCEPTION.name)
    retVal
  }
    
  @POST
  @Consumes(Array("application/json"))
  @Produces(Array("application/json"))
  @Path("logout")
  def getLogoutResponse(logoutRequest : LogoutRequest) : LogoutResponse = {
    UserManagementUtils.getIWebUMService().logout(logoutRequest)
    new LogoutResponse()
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
