package com.guavus.acume.core.configuration

import scala.collection.mutable.HashMap

import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.converter.AcumeDataSourceSchema
import com.guavus.qb.conf.QBConf
import com.guavus.qb.ds.DatasourceType
import com.guavus.qb.services.HBaseQueryBuilderService
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.qb.services.QueryBuilderService

object QueryBuilderFactory {

  def initializeQueryBuilderFactory(datasourceNames : Array[String], acumeContextMap: HashMap[String, AcumeContextTrait]) : HashMap[String, Seq[IQueryBuilderService]] = {
    val qbServiceMap = HashMap[String, Seq[IQueryBuilderService]]()
    
    datasourceNames.map(dsName => {
      val acumecontext = acumeContextMap.get(dsName).get
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
      qbServiceMap.+=(dsName -> List(qbService))
    })
    qbServiceMap
  }

}
