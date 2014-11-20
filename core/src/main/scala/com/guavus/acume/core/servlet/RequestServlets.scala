package com.guavus.acume.core.servlet

import java.io.Serializable
import com.guavus.acume.rest.beans.QueryJsonUtil
import com.guavus.acume.rest.beans.SearchRequest
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.Consumes
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.Produces
import com.guavus.acume.rest.beans.QueryRequest

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