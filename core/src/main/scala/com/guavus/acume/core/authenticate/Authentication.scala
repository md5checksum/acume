package com.guavus.acume.core.authenticate

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import util.log.LoggerUtils
import com.guavus.rubix.exceptions.RubixExceptionConstants
import com.guavus.rubix.user.management.UserManagementServiceF
import com.guavus.rubix.user.management.utils.HttpUtils
import com.sun.jersey.core.util.Base64
import com.guavus.rubix.RubixWebService.BadRequestException
import com.guavus.acume.core.webservice.HttpError

object Authentication {

  private var logger: Logger = LoggerFactory.getLogger(this.getClass())

  def authenticate(superParam: String, user: String, password: String) {
    if (superParam != null) {
      try {
        val tempSuperParam = Base64.base64Decode(superParam)
        val superUser = tempSuperParam.substring(0, tempSuperParam.indexOf('@'))
        val superPassword = tempSuperParam.substring(tempSuperParam.indexOf('@') + 1, tempSuperParam.indexOf('/'))
        val userName = tempSuperParam.substring(tempSuperParam.indexOf('/') + 1)
        UserManagementServiceF.getInstance.authenticate(superUser, superPassword)
        if (superUser == userName || UserManagementServiceF.getInstance.getUserByName(userName) != null) HttpUtils.setLoginInfo(userName)
      } catch {
        case e: Exception => {
          logger.error("Exception when decoding super user info from request parameters")
          LoggerUtils.printStackTraceInError(logger, e)
          throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.INVALID_CREDENTIALS.name(), "Authentication credentials bad or missing!", null)
        }
      }
    } else if (user != null && password != null) {
      try {
        UserManagementServiceF.getInstance.authenticate(user, password)
        HttpUtils.setLoginInfo(user)
      } catch {
        case e: Exception => {
          logger.error("Authentication failed for user : " + user)
          throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.INVALID_CREDENTIALS.name(), "Authentication credentials bad!", null)
        }
      }
    } else {
      logger.error("Authentication failed for user")
      throw new BadRequestException(HttpError.UNAUTHORISED, RubixExceptionConstants.MISSING_CREDENTIALS.name(), "Authentication credentials missing!", null)
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