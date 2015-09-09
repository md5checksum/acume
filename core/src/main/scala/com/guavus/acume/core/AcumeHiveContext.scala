package com.guavus.acume.core

import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
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
  
  AcumeContextTraitUtil.registerUserDefinedFunctions

}
