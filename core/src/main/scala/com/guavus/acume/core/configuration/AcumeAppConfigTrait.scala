package com.guavus.acume.core.configuration

import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.core.AcumeContext
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.DataService
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.rubix.user.permission.IPermissionTemplate
import com.guavus.acume.core.scheduler.Controller

/*
 * @author kashish.jain
 */
trait AcumeAppConfigTrait extends Serializable {
  
  def acumeService(dataService: DataService, datasourceName: String): AcumeService

  def dataService(queryBuilderService : Seq[IQueryBuilderService], ac : AcumeContextTrait, datasourceName: String): DataService 

  def defaultTimeGranularity(): TimeGranularity

  def acumeContext(datasourceName: String) : AcumeContextTrait

  def queryBuilderService(acumeContext : AcumeContextTrait, datasourceName: String) : Seq[IQueryBuilderService] 
  
  def permissionTemplate(): IPermissionTemplate 
  
  def queryRequestPrefetchTaskManager(acumeService : AcumeService, dataService : DataService , queryBuilderService : Seq[IQueryBuilderService], acumeContext : AcumeContextTrait, controller : Controller) : QueryRequestPrefetchTaskManager = throw new AbstractMethodError
  
  def controller(acumeContext : AcumeContextTrait) : Controller = throw new AbstractMethodError("Method not implemented.")
  
}