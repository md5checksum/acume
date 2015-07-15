package com.guavus.acume.core.authenticate

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import util.log.LoggerUtils
import com.guavus.rubix.user.management.UserManagementServiceF
import com.guavus.rubix.user.management.utils.HttpUtils
import com.sun.jersey.core.util.Base64
import com.guavus.acume.core.webservice.HttpError
import com.guavus.acume.core.webservice.BadRequestException
import com.guavus.acume.core.exceptions.AcumeExceptionConstants
import com.guavus.rubix.user.management.vo.LoginRequest
import com.guavus.rubix.user.management.exceptions.HttpUMException
import com.guavus.rubix.user.shiro.LoginRequestToken
import com.guavus.rubix.user.management.utils.UserManagementUtils
import org.apache.shiro.SecurityUtils
import com.guavus.rubix.user.management.exceptions.RubixExceptionConstants
import org.apache.shiro.subject.Subject
import org.apache.shiro.session.Session
import org.apache.shiro.authc.AuthenticationException

object Authentication {

  def authenticate(superParam: String, user: String, password: String) {
    
    var userNameToBeAuthenticated : String = null;
    var passwordToBeAuthenticated : String = null;

    //First check if the session if valid
    try{
      UserManagementUtils.getIWebUMService().validateSession(null)
      return
    } catch {
      case ex : HttpUMException => //logger.warn("Invalid session. Trying to authenticate through rubix db")
    }
    
    // Extract the userName and password from URL
    if (superParam != null) {
      try {
        val tempSuperParam = Base64.base64Decode(superParam)
        val superUser = tempSuperParam.substring(0, tempSuperParam.indexOf('@'))
        val superPassword = tempSuperParam.substring(tempSuperParam.indexOf('@') + 1, tempSuperParam.indexOf('/'))
        userNameToBeAuthenticated = superUser
        passwordToBeAuthenticated = superPassword
      } catch {
        case e: Exception => {
          throw new BadRequestException(HttpError.UNAUTHORISED, AcumeExceptionConstants.INVALID_CREDENTIALS.name, "Authentication credentials bad or missing!", null)
        }
      }
      
    } else if (user != null && password != null) {
      userNameToBeAuthenticated = user
      passwordToBeAuthenticated = password
      
    } else {
      throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.MISSING_CREDENTIALS.name(),"Authentication credentials missing!", null);
    }
    
    val loginRequest : LoginRequest = new LoginRequest()
    loginRequest.setUserName(userNameToBeAuthenticated)
    loginRequest.setPassword(passwordToBeAuthenticated)  
    val loginRequestToken : LoginRequestToken = new LoginRequestToken(loginRequest)
    val currentUser: Subject = SecurityUtils.getSubject()
    val currentSession: Session = currentUser.getSession(true)
    currentSession.setAttribute("AuthenticateOnly", "true")
    try {
      SecurityUtils.getSecurityManager().authenticate(loginRequestToken); 
      
    } catch {
      case ex : AuthenticationException => {
    	  throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.LOGIN_FAILED.name(),"Login failed. " + ex.getMessage(), null);
      }
    }

  }

/*
Original Java:
package com.guavus.acume.core.authenticate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.log.LoggerUtils;

import com.guavus.rubix.exceptions.RubixExceptionConstants;
import com.guavus.rubix.user.management.UserManagementServiceF;
import com.guavus.rubix.user.management.utils.HttpUtils;
import com.sun.jersey.core.util.Base64;

public class Authentication {
	private static Logger logger = LoggerFactory.getLogger(Authentication.class);
	public static void authenticate(String superParam, String user, String password) {
		if (superParam != null) {
			try {
				superParam = Base64.base64Decode(superParam);
				String superUser = superParam.substring(0, superParam.indexOf('@'));
				String superPassword = superParam.substring(superParam.indexOf('@') + 1, superParam.indexOf('/'));
				String userName = superParam.substring(superParam.indexOf('/') + 1);
				UserManagementServiceF.getInstance().authenticate(superUser, superPassword);
				if (superUser.equals(userName) || UserManagementServiceF.getInstance().getUserByName(userName) != null)
					HttpUtils.setLoginInfo(userName);
				
			} catch (Exception e) {
				logger.error("Exception when decoding super user info from request parameters");
				LoggerUtils.printStackTraceInError(logger, e);
				throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.INVALID_CREDENTIALS.name(), "Authentication credentials bad or missing!",null);
			}
			
		}
		else if(user != null && password != null){
			try { 
				UserManagementServiceF.getInstance().authenticate(user, password);
				HttpUtils.setLoginInfo(user);
			}
			catch (Exception e)
			{
				logger.error("Authentication failed for user : "+user);
				throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.INVALID_CREDENTIALS.name(), "Authentication credentials bad!", null);
			}
		}
		else{
			logger.error("Authentication failed for user");
			throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.MISSING_CREDENTIALS.name(),"Authentication credentials missing!", null);
		}
	}
}


*/
}
