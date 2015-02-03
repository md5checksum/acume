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
  
  def acumeService(dataService: DataService): AcumeService

  def dataService(queryBuilderService : Seq[IQueryBuilderService], ac : AcumeContextTrait): DataService 

  def defaultTimeGranularity(): TimeGranularity

  def acumeContext() : AcumeContextTrait

  def queryBuilderService(acumeContext : AcumeContextTrait) : Seq[IQueryBuilderService] 
  
  def permissionTemplate(): IPermissionTemplate 
  
  def queryRequestPrefetchTaskManager(acumeService : AcumeService, dataService : DataService , queryBuilderService : Seq[IQueryBuilderService], acumeContext : AcumeContext, controller : Controller) : QueryRequestPrefetchTaskManager = throw new AbstractMethodError
  
  def controller(acumeContext : AcumeContextTrait) : Controller = throw new AbstractMethodError("Method not implemented.")
  
}