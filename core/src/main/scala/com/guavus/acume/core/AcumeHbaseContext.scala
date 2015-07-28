package com.guavus.acume.core

import org.apache.spark.sql.hbase.HBaseSQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeHbaseCacheContext

class AcumeHbaseContext(val acumeConfiguration: AcumeConf) extends AcumeContextTrait {
  
  val hbaseContext = new HBaseSQLContext(sparkContext)
  val acumeCacheContext = new AcumeHbaseCacheContext(hbaseContext, new AcumeCacheConf)

  override def acc() = acumeCacheContext

  override def sqlContext() = hbaseContext
  
  def chooseHiveDatabase() {
    try{
       hbaseContext.sql("use " + acumeConfiguration.get(ConfConstants.backendDbName))
    } catch {
      case ex : Exception => throw new RuntimeException("Cannot use the database " + acumeConfiguration.get(ConfConstants.backendDbName), ex)
    }
  }
  
  chooseHiveDatabase()

}