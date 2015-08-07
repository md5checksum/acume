package com.guavus.acume.core.configuration

import scala.collection.mutable.HashMap

import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.DataService
import com.guavus.acume.core.scheduler.Controller
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.rubix.user.permission.IPermissionTemplate

/*
 * @author kashish.jain
 */
trait AcumeAppConfigTrait extends Serializable {
  /*
  def acumeService(dataService: DataService): AcumeService

  def dataService(queryBuilderService : Seq[IQueryBuilderService], ac : AcumeContextTrait): DataService 

  def defaultTimeGranularity(): TimeGranularity

  def acumeContext(dataSource : String) : AcumeContextTrait

  def queryBuilderService(dataSource : String) : Seq[IQueryBuilderService] 
  
  def permissionTemplate(): IPermissionTemplate 
  
  def queryRequestPrefetchTaskManager(acumeService : AcumeService, dataService : DataService , queryBuilderService : Seq[IQueryBuilderService], acumeContext : AcumeContextTrait, controller : Controller) : QueryRequestPrefetchTaskManager = throw new AbstractMethodError
  
  def controller(acumeContext : AcumeContextTrait) : Controller = throw new AbstractMethodError("Method not implemented.")
  
  def dataSource : String
  * 
  */
  
  def acumeService: AcumeService
  
  def dataServiceMap(queryBuilderServiceMap : HashMap[String, Seq[IQueryBuilderService]], acumeContextMap : HashMap[String, AcumeContextTrait]): HashMap[String, DataService]
  
  def defaultTimeGranularity(): TimeGranularity = TimeGranularity.HOUR
  
  def acumeContextMap(datasourceNames : Array[String]) : HashMap[String, AcumeContextTrait]
  
  def queryBuilderServiceMap(datasourceNames : Array[String], acumeContextMap : HashMap[String, AcumeContextTrait]) : HashMap[String, Seq[IQueryBuilderService]]
  
  def permissionTemplate(): IPermissionTemplate
  
  def queryRequestPrefetchTaskManager(queryBuilderServiceMap: HashMap[String, Seq[IQueryBuilderService]], acumeService : AcumeService, controller: Controller) : QueryRequestPrefetchTaskManager
  
  def controller(acumeContextMap : HashMap[String, AcumeContextTrait]) : Controller
  
  def datasourceNames : Array[String]

}