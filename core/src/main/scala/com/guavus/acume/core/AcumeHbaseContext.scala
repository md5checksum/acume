package com.guavus.acume.core

import org.apache.spark.sql.hbase.HBaseSQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeHbaseCacheContext

class AcumeHbaseContext(val datasoureName: String) extends AcumeContextTrait {
  
  val hbaseContext = AcumeContextTraitUtil.hBaseSQLContext
  val acumeCacheContext = new AcumeHbaseCacheContext(hbaseContext, new AcumeCacheConf(datasoureName))

  override def acc() = acumeCacheContext

  override def sqlContext() = hbaseContext
  
  def chooseHiveDatabase() {
    try{
       hbaseContext.sql("use " + acumeConf.get(ConfConstants.backendDbName))
    } catch {
      case ex : Exception => throw new RuntimeException("Cannot use the database " + acumeConf.get(ConfConstants.backendDbName), ex)
    }
  }
  
  chooseHiveDatabase()

}