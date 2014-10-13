package com.guavus.acume.core.servletfilters

import java.io.IOException
import javax.servlet.Filter
import javax.servlet.FilterChain
import javax.servlet.FilterConfig
import javax.servlet.ServletException
import javax.servlet.ServletRequest
import javax.servlet.ServletResponse
import javax.servlet.http.HttpServletRequest
import org.apache.shiro.SecurityUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.google.common.base.Throwables
import com.guavus.rubix.RubixWebService.Authentication
import com.guavus.rubix.user.management.UserManagementServiceF
import com.guavus.rubix.user.management.utils.HttpUtils
import com.guavus.rubix.usermanagement.UsrManagementService
import com.sun.jersey.core.util.Base64
import com.guavus.rubix.user.management.service.UserManagementService
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.rubix.user.permission.IPermissionTemplate

/**
 * Sets the username into this thread context
 */
class SessionFilter extends Filter {

  private var logger: Logger = LoggerFactory.getLogger(classOf[SessionFilter])

  private var umService: UserManagementService = new UserManagementService(ConfigFactory.getInstance.getBean(classOf[IPermissionTemplate]))

  override def init(filterConfig: FilterConfig) {
  }

  override def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    activateHttpUtils(httpRequest)
    chain.doFilter(request, response)
    HttpUtils.recycle()
  }

  private def activateHttpUtils(request: HttpServletRequest) {
    val username = SecurityUtils.getSubject.getSession(true).getAttribute("login").asInstanceOf[String]
    HttpUtils.setLoginInfo(username)
    umService.validateResourceAccess(request.getRequestURI)
  }

  override def destroy() {
  }

/*
Original Java:
package com.guavus.rubix.servletfilters;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.shiro.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.guavus.rubix.RubixWebService.Authentication;
import com.guavus.rubix.user.management.UserManagementServiceF;
import com.guavus.rubix.user.management.utils.HttpUtils;
import com.guavus.rubix.usermanagement.UsrManagementService;
import com.sun.jersey.core.util.Base64;


public class SessionFilter implements Filter {

	private Logger logger = LoggerFactory.getLogger(SessionFilter.class);
	private UsrManagementService umService = new UsrManagementService();
	
	@Override
	public void init(FilterConfig filterConfig) throws ServletException {

	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {		
		HttpServletRequest httpRequest = (HttpServletRequest)request;			
		activateHttpUtils(httpRequest);
		chain.doFilter(request, response);
		HttpUtils.recycle();

	}


	private void activateHttpUtils(HttpServletRequest request) {
		
		String username =  
				(String)SecurityUtils.getSubject().getSession(true).getAttribute("login");
		HttpUtils.setLoginInfo(username);
		umService.validateResourceAccess(request.getRequestURI());
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub

	}

}

*/
}