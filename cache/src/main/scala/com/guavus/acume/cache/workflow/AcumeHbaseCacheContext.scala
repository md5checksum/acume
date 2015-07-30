package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.Utility

class AcumeHbaseCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) extends AcumeCacheContextTrait {

  sqlContext match {
    case hiveContext: HiveContext =>
    case hbaseContext : HBaseSQLContext =>
    case sqlContext: SQLContext =>
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  Utility.init(conf)
  Utility.unmarshalXML(conf.get(ConfConstants.businesscubexml), dimensionMap, measureMap)
  
  private [acume] def cacheSqlContext() : SQLContext = sqlContext
  private [acume] def cacheConf = conf
  
  override val dataLoader = throw new RuntimeException("Operation not supported")
  
  override private [acume] def executeQuery(sql: String) = {
    val resultSchemaRDD = sqlContext.sql(sql)
    new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD.rdd, MetaData(-1, Nil))
  }

  private [acume] def getCubeMap = throw new RuntimeException("Operation not supported")
  
}