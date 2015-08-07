package com.guavus.acume.core.configuration

import scala.collection.mutable.HashMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.AcumeContextTraitUtil
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.DataService
import com.guavus.acume.core.scheduler.Controller
import com.guavus.acume.core.scheduler.ISchedulerPolicy
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager
import com.guavus.acume.core.usermanagement.DefaultPermissionTemplate
import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.rubix.user.permission.IPermissionTemplate
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.insta.Insta
import com.guavus.acume.cache.disk.utility.InstaUtil

object AcumeAppConfig {

  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeAppConfig])

  var DEFAULT_CLASS_LOADER: ClassLoader = Thread.currentThread().getContextClassLoader
}

@org.springframework.context.annotation.Configuration
class AcumeAppConfig extends AcumeAppConfigTrait {
  
  @Bean
  @Autowired
  override def acumeService: AcumeService = {
    new AcumeService
  }

  @Bean
  @Autowired
  override def dataServiceMap(queryBuilderServiceMap : HashMap[String, Seq[IQueryBuilderService]], acumeContextMap : HashMap[String, AcumeContextTrait]): HashMap[String, DataService] = {
   DataServiceFactory.initDataServiceFactory(queryBuilderServiceMap, acumeContextMap)
  }

  @Bean
  override def defaultTimeGranularity(): TimeGranularity = TimeGranularity.HOUR
  
  @Bean
  @Autowired
  override def acumeContextMap(datasourceNames : Array[String]) : HashMap[String, AcumeContextTrait] = {
    AcumeContextTraitUtil.initAcumeContextTraitFactory(datasourceNames)
  }
  
  @Bean
  @Autowired
  override def queryBuilderServiceMap(datasourceNames : Array[String], acumeContextMap : HashMap[String, AcumeContextTrait]) : HashMap[String, Seq[IQueryBuilderService]] = {
    QueryBuilderFactory.initializeQueryBuilderFactory(datasourceNames, acumeContextMap)
  }

  @Bean
  override def permissionTemplate(): IPermissionTemplate = new DefaultPermissionTemplate()
  
  @Bean
  @Autowired
  override def queryRequestPrefetchTaskManager(queryBuilderServiceMap: HashMap[String, Seq[IQueryBuilderService]], acumeService : AcumeService, controller: Controller) : QueryRequestPrefetchTaskManager = {
    val ischedulerpolicy = ISchedulerPolicy.getISchedulerPolicy
    val qbSchemaList = List[QueryBuilderSchema]()
    
    queryBuilderServiceMap.map(entry => {
      qbSchemaList.++(entry._2.map(_.getQueryBuilderSchema).toList)
    })
    new QueryRequestPrefetchTaskManager(qbSchemaList, acumeService, ischedulerpolicy, controller)
  }
  
  @Bean
  @Autowired
  override def controller(acumeContextMap : HashMap[String, AcumeContextTrait]) : Controller = {
    new Controller()
  }
  
  @Bean
  @Autowired
  override def datasourceNames : Array[String] = {
    AcumeContextTraitUtil.acumeConf.getAllDatasourceNames
  }
  
}