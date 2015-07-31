package com.guavus.acume.core.configuration

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
import com.guavus.acume.core.listener.AcumeBlockManagerRemovedListener
import com.guavus.acume.core.scheduler.Controller
import com.guavus.acume.core.scheduler.ISchedulerPolicy
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager
import com.guavus.acume.core.usermanagement.DefaultPermissionTemplate
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.rubix.user.permission.IPermissionTemplate

object AcumeAppConfig {

  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeAppConfig])

  var DEFAULT_CLASS_LOADER: ClassLoader = Thread.currentThread().getContextClassLoader
}

@org.springframework.context.annotation.Configuration
class AcumeAppConfig extends AcumeAppConfigTrait {

  @Bean
  @Autowired
  private val dataSource : String = AcumeContextTraitUtil.acumeConf.getAllDatasourceNames(0)
  
  @Bean
  @Autowired
  override def acumeService(dataService: DataService, dataSource: String): AcumeService = {
    new AcumeService(dataService)
  }

  @Bean
  @Autowired
  override def dataService(queryBuilderService : Seq[IQueryBuilderService], ac : AcumeContextTrait, dataSource: String): DataService = {
    new DataService(queryBuilderService, ac)
  }

  @Bean
  override def defaultTimeGranularity(): TimeGranularity = TimeGranularity.HOUR
  
  @Bean
  @Autowired
  override def acumeContext(dataSource: String) : AcumeContextTrait = {
    AcumeContextTraitUtil.getAcumeContext(dataSource)
  }
  
  @Bean
  @Autowired
  override def queryBuilderService(acumeContext : AcumeContextTrait, dataSource: String) : Seq[IQueryBuilderService] = {
    List(QueryBuilderFactory.getQBInstance(dataSource))
  }

  @Bean
  override def permissionTemplate(): IPermissionTemplate = new DefaultPermissionTemplate()
  
  @Bean
  @Autowired
  override def queryRequestPrefetchTaskManager(acumeService : AcumeService, dataService : DataService , queryBuilderService : Seq[IQueryBuilderService], acumeContext : AcumeContextTrait, controller : Controller) : QueryRequestPrefetchTaskManager = {
    acumeContext.sc.addSparkListener(new AcumeBlockManagerRemovedListener)
    val ischedulerpolicy = ISchedulerPolicy.getISchedulerPolicy(acumeContext.acumeConf)
    new QueryRequestPrefetchTaskManager(dataService, queryBuilderService.map(_.getQueryBuilderSchema).toList, acumeContext, acumeService, ischedulerpolicy, controller)
  }
  
  @Bean
  @Autowired
  override def controller(acumeContext : AcumeContextTrait) : Controller = {
    new Controller(acumeContext.acc)
  } 
  
  
  
}