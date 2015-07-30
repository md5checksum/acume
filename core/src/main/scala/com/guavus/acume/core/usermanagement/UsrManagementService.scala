package com.guavus.acume.core.usermanagement

import com.guavus.rubix.user.management.service.UserManagementService
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.rubix.user.permission.IPermissionTemplate


class UsrManagementService extends UserManagementService(ConfigFactory.getInstance().getBean(classOf[IPermissionTemplate])) {
}