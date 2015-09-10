package com.guavus.acume.core.servlet

import java.io.Serializable
import com.guavus.rubix.query.remote.flex.QueryJsonUtil
import com.guavus.rubix.query.remote.flex.QueryRequest
import com.guavus.rubix.query.remote.flex.SearchRequest
import javax.servlet.http.HttpServletRequest
import com.guavus.rubix.query.remote.flex.ZoneInfoRequest
import com.guavus.rubix.user.management.vo.ValidateSessionRequest
import com.guavus.rubix.query.remote.flex.LoginParameterRequest


class SearchRequestServlet extends AbstractRequestServlet {

  override def getResponse(req : HttpServletRequest) : Serializable = {
   
    val searchRequest : SearchRequest = QueryJsonUtil.fromJsonToSearchRequest(req.getReader().readLine())
    service.servSearchQuery(searchRequest,  req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
  }
 
}

class SqlRequestServlet extends AbstractRequestServlet {

  override def getResponse(req : HttpServletRequest) : Serializable = {
    service.servSqlQuery(req.getReader().readLine(),  req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
  }
 
}

class RaeSqlRequestServlet extends AbstractRequestServlet {

  override def getResponse(req : HttpServletRequest) : Serializable = {
    service.raeServSqlQueryOnDataSource(req.getReader().readLine(),  req.getParameter("dataSource"), req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
  }
}

class AggregateRequestServlet extends AbstractRequestServlet {

  override def getResponse(req : HttpServletRequest) : Serializable = {
    
    val queryRequest : QueryRequest = QueryJsonUtil.fromJsonToQueryRequest(req.getReader().readLine())
    service.servAggregate(queryRequest,  req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
  
  }
 
}

class TimeSeriesRequestServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    val queryRequest : QueryRequest = QueryJsonUtil.fromJsonToQueryRequest(req.getReader().readLine())
    service.servTimeseries(queryRequest,  req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
  }
 
}

class TimeSeriesMultipleRequestServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    val queryRequests  = QueryJsonUtil.fromJsonToQueryRequests(req.getReader().readLine())
    service.servTimeseriesMultiple(queryRequests,  req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
  }
 
}

class AggregateMultipleRequestServlet extends AbstractRequestServlet {

  override def getResponse(req : HttpServletRequest) : Serializable = {
    
    val queryRequests = QueryJsonUtil.fromJsonToQueryRequests(req.getReader().readLine())
    service.servAggregateMultiple(queryRequests,  req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
  
  }
 
}

class ValidateQueryServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    service.isValidQuery(req.getReader().readLine(),  req.getParameter("dataSource"), req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false).asInstanceOf[Serializable]
  }
 
}


class ValidDataSourcesServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    service.getValidDataSources(req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false).asInstanceOf[Serializable]
  }
 
}

class DataAvailabilityServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    service.getDataAvailability(req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false).asInstanceOf[Serializable]
   }
}

class ValidateSessionServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    val validateSessionRequest : ValidateSessionRequest = QueryJsonUtil.fromJsonToValidateSessionRequest(req.getReader().readLine())
    service.getValidSession(validateSessionRequest).asInstanceOf[Serializable]
  }
 
}
   
class LoginRequestServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    val loginParameterRequest : LoginParameterRequest = QueryJsonUtil.fromJsonToLoginParameterRequest(req.getReader().readLine())
    service.getLoginResponse(loginParameterRequest).asInstanceOf[Serializable]
  }
 
}

class ZoneInfoRequestServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    val zoneInfo : ZoneInfoRequest = QueryJsonUtil.fromJsonToZoneInfoRequest(req.getReader().readLine())
    service.getZoneInfo(zoneInfo, req.getParameter("super"),req.getParameter("user"), req.getParameter("password")).asInstanceOf[Serializable]
  }
 
}

class TimeRangeRequestServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    service.getTimeRange(req.getParameter("super"),req.getParameter("user"), req.getParameter("password")).asInstanceOf[Serializable]
  }
 
}

class InstaAvailabilityRequestServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    service.getInstaAvailabilty(req.getParameter("super"),req.getParameter("user"), req.getParameter("password"), req.getParameter("binSource")).asInstanceOf[Serializable]
  }
 
}

class AcumeAvailabilityRequestServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    service.getAcumeAvailabilty(req.getParameter("super"),req.getParameter("user"), req.getParameter("password")).asInstanceOf[Serializable]
  }
 
}

class UnionizedCacheAvailabilityServlet extends AbstractRequestServlet {

   override def getResponse(req : HttpServletRequest) : Serializable = {
    service.getUnionizedCacheAvailability(req.getParameter("super"),req.getParameter("user"), req.getParameter("password")).asInstanceOf[Serializable]
  }
 
}

