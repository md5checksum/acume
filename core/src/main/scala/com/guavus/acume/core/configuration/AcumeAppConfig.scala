package com.guavus.acume.core.configuration

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.core.TimeGranularity.TimeGranularity
import com.guavus.acume.core.AcumeContextTraitUtil
import com.guavus.acume.core.AcumeService
import com.guavus.acume.core.scheduler.Controller
import com.guavus.acume.core.scheduler.ISchedulerPolicy
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager
import com.guavus.acume.core.usermanagement.DefaultPermissionTemplate
import com.guavus.qb.cube.schema.QueryBuilderSchema
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
  override def acumeService: AcumeService = {
    new AcumeService
  }

  @Bean
  @Autowired
  override def dataServiceMap(queryBuilderServiceMap : QueryBuilderSerciceMap, acumeContextMap : AcumeContextTraitMap): DataServiceMap = {
   DataServiceMap(DataServiceFactory.initDataServiceFactory(queryBuilderServiceMap.q, acumeContextMap.a))
  }

  @Bean
  override def defaultTimeGranularity(): TimeGranularity = TimeGranularity.HOUR
  
  @Bean
  @Autowired
  override def acumeContextMap(datasourceNames : DataSourceNames) : AcumeContextTraitMap = {
    AcumeContextTraitMap(AcumeContextTraitUtil.initAcumeContextTraitFactory(datasourceNames.d))
  }
  
  @Bean
  @Autowired
  override def queryBuilderServiceMap(datasourceNames : DataSourceNames, acumeContextMap : AcumeContextTraitMap) : QueryBuilderSerciceMap = {
    QueryBuilderSerciceMap(QueryBuilderFactory.initializeQueryBuilderFactory(datasourceNames.d, acumeContextMap.a))
  }

  @Bean
  override def permissionTemplate(): IPermissionTemplate = new DefaultPermissionTemplate()
  
  @Bean
  @Autowired
  override def queryRequestPrefetchTaskManager(queryBuilderServiceMap: QueryBuilderSerciceMap, acumeService : AcumeService, controller: Controller) : QueryRequestPrefetchTaskManager = {
    val ischedulerpolicy = ISchedulerPolicy.getISchedulerPolicy
    val qbSchemaList = List[QueryBuilderSchema]()
    
    queryBuilderServiceMap.q.map(entry => {
      qbSchemaList.++(entry._2.map(_.getQueryBuilderSchema).toList)
    })
    new QueryRequestPrefetchTaskManager(qbSchemaList, acumeService, ischedulerpolicy, controller)
  }
  
  @Bean
  @Autowired
  override def controller(acumeContextMap : AcumeContextTraitMap) : Controller = {
    new Controller()
  }
  
  @Bean
  @Autowired
  override def datasourceNames : DataSourceNames = {
    DataSourceNames(AcumeContextTraitUtil.acumeConf.getAllDatasourceNames)
  }
  
}