package com.guavus.acume.core.servlet

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletRequest
import com.guavus.acume.rest.api.RestService
import com.guavus.acume.rest.beans.QueryJsonUtil
import com.guavus.acume.rest.beans.AggregateResponse
import com.guavus.acume.rest.beans.TimeseriesResponse

class SqlRequestServlet extends HttpServlet {

  val service = new RestService()
  override def doPost(req: HttpServletRequest, resp: HttpServletResponse) {
    
	  val response = service.servSqlQuery(req.getReader().readLine(),  req.getParameter("super"),
			req.getParameter("user"), req.getParameter("password"), false)
			var finalResponse :String = null
			if(response.isInstanceOf[AggregateResponse]) {
			  finalResponse = AggregateResponse.gson.toJson(response)
			} else if(response.isInstanceOf[TimeseriesResponse]) {
			  finalResponse = TimeseriesResponse.gson.toJson(response)
			} else {
			  finalResponse = TimeseriesResponse.gson.toJson(response)
			}
			resp.getOutputStream().print(finalResponse)
			resp.flushBuffer()
  }
}