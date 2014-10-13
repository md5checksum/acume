package com.guavus.acume.core.servletfilters

import javax.servlet._
import javax.servlet.http.HttpServletResponse

class CacheControlFilter extends Filter {

  def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
    val resp = response.asInstanceOf[HttpServletResponse]
    resp.setHeader("Cache-Control", "no-store")
    resp.setHeader("Pragma", "public")
    chain.doFilter(request, response)
  }

  override def init(filterConfig: FilterConfig) {
  }

  override def destroy() {
  }

/*
Original Java:
package com.guavus.rubix.servletfilters;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.io.IOException;

public class CacheControlFilter implements Filter {

    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {

        HttpServletResponse resp = (HttpServletResponse) response;
        
        |*
         * fix for a bug in IE8 when using https, where resources do not get downloaded 
         * when Cache-Control or Pragma is no-cache
         *|
        resp.setHeader("Cache-Control", "no-store");
	    resp.setHeader("Pragma", "public");

        chain.doFilter(request, response);
    }

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}

}

*/
}