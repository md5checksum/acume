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

  val hiveContext = new HiveContext(sparkContext)
  val acumeCacheContext = new AcumeHiveCacheContext(hiveContext, new AcumeCacheConf)
 
  override def acc() = acumeCacheContext
  
  override def sqlContext() = hiveContext
  
  def chooseHiveDatabase() {
    try{
       hiveContext.sql("use " + acumeConfiguration.get(ConfConstants.backendDbName))
    } catch {
      case ex : Exception => throw new RuntimeException("Cannot use the database " + acumeConfiguration.get(ConfConstants.backendDbName), ex)
    }
  }
  
  chooseHiveDatabase()

}
