package com.guavus.acume.core.configuration

import com.guavus.acume.core.AcumeContextTraitUtil
import com.guavus.acume.core.converter.AcumeDataSourceSchema
import com.guavus.qb.conf.QBConf
import com.guavus.qb.ds.DatasourceType
import com.guavus.qb.services.HBaseQueryBuilderService
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.qb.services.QueryBuilderService

object QueryBuilderFactory {
  
  private var qbServiceMap = Map[String, IQueryBuilderService]()
  
  AcumeContextTraitUtil.acumeConf.getAllDatasourceNames.map(dsName => {
    val acumecontext = AcumeContextTraitUtil.getAcumeContext(dsName)

    val qbService: IQueryBuilderService =
      DatasourceType.getDataSourceTypeFromString(dsName.toLowerCase) match {
        case DatasourceType.CACHE =>
          new QueryBuilderService(new AcumeDataSourceSchema(acumecontext), new QBConf())
        case DatasourceType.HIVE =>
          new QueryBuilderService(new AcumeDataSourceSchema(acumecontext), new QBConf())
        case DatasourceType.HBASE =>
         new HBaseQueryBuilderService(new AcumeDataSourceSchema(acumecontext), new QBConf())
        case _ => throw new RuntimeException("wrong datasource configured")
    }
    
    qbServiceMap = qbServiceMap.+(dsName -> qbService)
  })
  
  def getQBInstance(datasoureName : String) : IQueryBuilderService = {
    qbServiceMap.getOrElse(datasoureName, throw new RuntimeException(s"Datasource $datasoureName not configured"))
  }

}
