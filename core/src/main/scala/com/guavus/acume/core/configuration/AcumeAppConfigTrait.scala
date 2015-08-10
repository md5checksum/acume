package com.guavus.acume.core.configuration

import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.scheduler.Controller
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager
import com.guavus.rubix.user.permission.IPermissionTemplate

/*
 * @author kashish.jain
 */
trait AcumeAppConfigTrait extends Serializable {
  /*
   * Old Set of beans. 
   * /
  def acumeService(dataService: DataService): AcumeService

  def dataService(queryBuilderService : Seq[IQueryBuilderService], ac : AcumeContextTrait): DataService 

  def defaultTimeGranularity(): TimeGranularity

  def acumeContext(dataSource : String) : AcumeContextTrait

  def queryBuilderService(dataSource : String) : Seq[IQueryBuilderService] 
  
  def permissionTemplate(): IPermissionTemplate 
  
  def queryRequestPrefetchTaskManager(acumeService : AcumeService, dataService : DataService , queryBuilderService : Seq[IQueryBuilderService], acumeContext : AcumeContextTrait, controller : Controller) : QueryRequestPrefetchTaskManager = throw new AbstractMethodError
  
  def controller(acumeContext : AcumeContextTrait) : Controller = throw new AbstractMethodError("Method not implemented.")
  
  */
  
  /*
   *  New set of beans for multiple datasaource support
   */
  def acumeService: AcumeService
  
  def dataServiceMap(queryBuilderServiceMap : QueryBuilderSerciceMap, acumeContextMap : AcumeContextTraitMap): DataServiceMap

  def defaultTimeGranularity(): TimeGranularity
  
  def acumeContextMap(datasourceNames : DataSourceNames) : AcumeContextTraitMap 
  
  def queryBuilderServiceMap(datasourceNames : DataSourceNames, acumeContextMap : AcumeContextTraitMap) : QueryBuilderSerciceMap 
  
  def permissionTemplate(): IPermissionTemplate
  
  def queryRequestPrefetchTaskManager(queryBuilderServiceMap: QueryBuilderSerciceMap, acumeService : AcumeService, controller: Controller) : QueryRequestPrefetchTaskManager 
  
  def controller(acumeContextMap : AcumeContextTraitMap) : Controller 
  
  def datasourceNames : DataSourceNames
  
}