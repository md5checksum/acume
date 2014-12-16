package com.guavus.acume.core.usermanagement

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.rubix.user.permission.IPermissionTemplate
import com.guavus.rubix.user.shiro.UserRealm
import DefaultUserRealm._
import com.guavus.acume.core.configuration.ConfigFactory

object DefaultUserRealm {

  val logger = LoggerFactory.getLogger(classOf[DefaultUserRealm])

  private var template: IPermissionTemplate = _

  try {
    template = ConfigFactory.getInstance.getBean(classOf[IPermissionTemplate])
  } catch {
    case e: Exception => {
      logger.error("Error in initializing DefaultUserRelam" + e)
      throw new IllegalStateException(e)
    }
  }
}

class DefaultUserRealm extends UserRealm {

  override def getPermissionTemplate(): IPermissionTemplate = template

/*
Original Java:
package com.guavus.rubix.usermanagement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guavus.rubix.configuration.ConfigFactory;
import com.guavus.rubix.user.permission.IPermissionTemplate;
import com.guavus.rubix.user.shiro.UserRealm;

public class DefaultUserRealm extends UserRealm {
	public static final Logger logger = LoggerFactory
			.getLogger(DefaultUserRealm.class);

	private static IPermissionTemplate template;

	static {
		try {
			template = ConfigFactory.getInstance().getBean(
					IPermissionTemplate.class);
		} catch (Exception e) {
			logger.error("Error in initializing DefaultUserRelam" + e);
			throw new IllegalStateException(e);
		}
	}

	@Override
	public IPermissionTemplate getPermissionTemplate() {
		return template;
	}

}

*/
}