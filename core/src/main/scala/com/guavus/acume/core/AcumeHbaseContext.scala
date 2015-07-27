package com.guavus.acume.core

import org.apache.spark.sql.hbase.HBaseSQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeHbaseCacheContext

class AcumeHbaseContext(val acumeConfiguration: AcumeConf) extends AcumeContextTrait {
  
  val _sqlContext = new HBaseSQLContext(sparkContext)
  val acumeContext = new AcumeHbaseCacheContext(_sqlContext, new AcumeCacheConf)

  override def ac() = acumeContext

  override def sqlContext() = _sqlContext
  
  def chooseHiveDatabase() {
    try{
       _sqlContext.sql("use " + acumeConfiguration.get(ConfConstants.backendDbName))
    } catch {
      case ex : Exception => throw new RuntimeException("Cannot use the database " + acumeConfiguration.get(ConfConstants.backendDbName), ex)
    }
  }
  
  chooseHiveDatabase()

}