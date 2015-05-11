package com.guavus.rubix.permission

import org.apache.shiro.authz.Permission
import org.apache.shiro.authz.permission.PermissionResolver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.rubix.user.management.utils.ServicePermissionUtil
import com.guavus.rubix.user.permission.IPermissionTemplate
import com.guavus.rubix.user.permission.impl.ServiceOperationConstants
import RubixPermissionResolver._
import com.guavus.acume.core.configuration.ConfigFactory

object RubixPermissionResolver {

  private val PERMISSION_STRING_SEPARATOR = ":"

  private var logger: Logger = LoggerFactory.getLogger(classOf[RubixPermissionResolver])
}

class RubixPermissionResolver extends PermissionResolver {

  override def resolvePermission(permissionString: String): Permission = {
    try {
    logger.debug("resolving permission for string {}", permissionString)
    val serviceTypeAndOperation = permissionString.split(PERMISSION_STRING_SEPARATOR)
    if (serviceTypeAndOperation.length != 2) throw new IllegalArgumentException("Expected permission string of the form SERVICE_TYPE:OPERATION_VALUE")
    val permissionTemplate = ConfigFactory.getInstance.getBean(classOf[IPermissionTemplate])
    ServicePermissionUtil.getInstance(permissionTemplate)
				.resolvePermission(
						serviceTypeAndOperation(0),
						ServiceOperationConstants.valueOf(
								serviceTypeAndOperation(1)).getValue());
    } catch {
      case e : Throwable => e.printStackTrace()
      throw e
    }
  }

/*
Original Java:
package com.guavus.rubix.permission;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guavus.rubix.configuration.ConfigFactory;
import com.guavus.rubix.user.management.utils.ServicePermissionUtil;
import com.guavus.rubix.user.permission.IPermissionTemplate;
import com.guavus.rubix.user.permission.impl.ServiceOperationConstants;

public class RubixPermissionResolver implements PermissionResolver {
	
	private static final String PERMISSION_STRING_SEPARATOR = ":";
	private static Logger logger = LoggerFactory.getLogger(RubixPermissionResolver.class); 

	@Override
	public Permission resolvePermission(String permissionString) {
		logger.debug("resolving permission for string {}", permissionString);
		String[] serviceTypeAndOperation = permissionString.split(PERMISSION_STRING_SEPARATOR);
		if (serviceTypeAndOperation.length != 2)
			throw new IllegalArgumentException(
					"Expected permission string of the form SERVICE_TYPE:OPERATION_VALUE");
		
		IPermissionTemplate permissionTemplate = ConfigFactory.getInstance().getBean(IPermissionTemplate.class);
		return ServicePermissionUtil.getInstance(permissionTemplate)
				.resolvePermission(
						serviceTypeAndOperation[0],
						ServiceOperationConstants.valueOf(
								serviceTypeAndOperation[1]).getValue());
	}

}

*/
}