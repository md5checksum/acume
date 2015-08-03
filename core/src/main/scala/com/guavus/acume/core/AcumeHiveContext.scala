package com.guavus.acume.core

import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeHiveCacheContext

/**
 * @author kashish.jain
 * 
 */
class AcumeHiveContext(val datasourceName: String) extends AcumeContextTrait {

  private val hiveContext = AcumeContextTraitUtil.hiveContext
  private val acumeCacheContext = new AcumeHiveCacheContext(hiveContext, new AcumeCacheConf(datasourceName))
 
  override def acc() = acumeCacheContext
  
  override def sqlContext() = hiveContext
  
  val chooseHiveDatabase = {
    try{
       hiveContext.sql("use " + acumeConf.get(ConfConstants.backendDbName))
    } catch {
      case ex : Exception => throw new RuntimeException("Cannot use the database " + acumeConf.get(ConfConstants.backendDbName), ex)
    }
  }
  
  AcumeContextTraitUtil.registerUserDefinedFunctions

}
