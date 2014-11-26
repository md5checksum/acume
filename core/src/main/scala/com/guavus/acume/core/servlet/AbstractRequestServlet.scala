package com.guavus.acume.core.servlet

import com.guavus.acume.rest.api.RestService
import com.guavus.acume.rest.beans.AggregateResponse
import com.guavus.acume.rest.beans.TimeseriesResponse
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.Path
import javax.xml.bind.annotation.XmlRootElement
import com.guavus.acume.rest.beans.SearchResponse
import javax.servlet.ServletException
import java.io.Serializable

abstract class AbstractRequestServlet extends HttpServlet {

  val service = new RestService()
  
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    
    val response = getResponse(req)
    
    var finalResponse :String = null
	
    if(response.isInstanceOf[AggregateResponse]) {
	  finalResponse = AggregateResponse.gson.toJson(response)
	} else if(response.isInstanceOf[TimeseriesResponse]) {
	  finalResponse = TimeseriesResponse.gson.toJson(response)
	} else if(response.isInstanceOf[SearchResponse]) {
	  //search response
	  finalResponse = TimeseriesResponse.gson.toJson(response)
	} else if(response.isInstanceOf[Serializable]) {
	  finalResponse = response.toString
	} else {
	  throw new ServletException("Invalid response");
	}
    
	resp.getOutputStream().print(finalResponse)
	resp.flushBuffer()
    
  }
  
  def getResponse(req: HttpServletRequest) : Serializable
  
}