package com.guavus.acume.core.configuration

import com.guavus.qb.services.IQueryBuilderService
import com.guavus.qb.ds.DatasourceType
import com.guavus.qb.services.HBaseQueryBuilderService
import com.guavus.qb.services.QueryBuilderService
import com.guavus.acume.core.converter.AcumeDataSourceSchema
import com.guavus.acume.core.AcumeContextTraitUtil
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.qb.conf.QBConf

object QueryBuilderFactory {
  
  var qbServiceMap = Map[String, IQueryBuilderService]()
  
  private def createNewQbInstance(datasourceName: String) : IQueryBuilderService = {
   
    var qbService : IQueryBuilderService = null
    var acumecontext : AcumeContextTrait = AcumeContextTraitUtil.acumeContextMap.get(datasourceName).getOrElse(throw new RuntimeException("datasource not configured"))
   
    DatasourceType.getDataSourceTypeFromString(datasourceName) match {
     case DatasourceType.CACHE => 
       qbService = new QueryBuilderService(new AcumeDataSourceSchema(acumecontext), new QBConf())
     case DatasourceType.HIVE =>
       qbService = new QueryBuilderService(new AcumeDataSourceSchema(acumecontext), new QBConf())
     case DatasourceType.HBASE =>
       qbService = new HBaseQueryBuilderService(new AcumeDataSourceSchema(acumecontext), new QBConf())
     case _ => throw new RuntimeException("datasource not configured")
    }
    
    qbService
  }
  
  def getQBInstance(datasoureName : String) : IQueryBuilderService = {
    qbServiceMap.get(datasoureName).getOrElse(createNewQbInstance(datasoureName))
  }

}
