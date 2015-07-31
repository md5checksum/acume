package com.guavus.acume.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.guavus.acume.core.listener.AcumeSparkListener
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import com.guavus.acume.core.configuration.QueryBuilderFactory
import com.guavus.qb.ds.DatasourceType

object AcumeContextTraitUtil {
  
  // Initialize sparkContext, hiveContext, HbaseSQLContext only  once
  val sparkContext = new SparkContext(new SparkConf())
  sparkContext.addSparkListener(new AcumeSparkListener)
  var hiveContext : HiveContext = null
  var hBaseSQLContext : HBaseSQLContext = null
  
  var acumeContextMap = Map[String, AcumeContextTrait]() 
  
  val acumeConf = new AcumeConf(true, "/acume.ini")
  
  acumeConf.getAllDatasourceNames.map(dsName => {
    var context : Option[AcumeContextTrait] = null
    DatasourceType.getDataSourceTypeFromString(dsName.toLowerCase) match {
     case DatasourceType.CACHE => 
        if(hiveContext == null)
          hiveContext =  new HiveContext(sparkContext)
        context = Some(new AcumeContext(dsName.toLowerCase))
      case DatasourceType.HIVE =>
        if(hiveContext == null)
          hiveContext =  new HiveContext(sparkContext)
        context = Some(new AcumeHiveContext(dsName.toLowerCase))
      case DatasourceType.HBASE =>
        if(hBaseSQLContext == null)
          hBaseSQLContext =  new HBaseSQLContext(sparkContext)
        context = Some(new AcumeHbaseContext(dsName.toLowerCase))
      case _ => throw new RuntimeException("wrong datasource configured")
    }
    context.get.init
    acumeContextMap.+=(dsName.toLowerCase -> context.get)
    QueryBuilderFactory.getQBInstance(dsName)
  })
  
  def getAcumeContext(dsName : String) : AcumeContextTrait = {
    acumeContextMap.get(dsName.toLowerCase).getOrElse(throw new RuntimeException("This datasource is not configured"))
  }
  
}