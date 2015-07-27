package com.guavus.acume.core

import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeHiveCacheContext

/**
 * @author kashish.jain
 * 
 */
class AcumeHiveContext(val acumeConfiguration: AcumeConf) extends AcumeContextTrait {

  val _sqlContext = new HiveContext(sparkContext)
  val acumeContext = new AcumeHiveCacheContext(_sqlContext, new AcumeCacheConf)
 
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
