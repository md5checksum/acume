package com.guavus.acume.core

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.workflow.AcumeCacheContext

/**
 * @author pankaj.arora
 *
 * This will keep the acumeCacheContextTrait and hive context.
 */
class AcumeContext(val datasourceName : String) extends AcumeContextTrait {

  private val _hiveContext = AcumeContextTraitUtil.hiveContext
  private val acumeCacheContext = new AcumeCacheContext(_hiveContext, new AcumeCacheConf(datasourceName))
  
  override def acc() = acumeCacheContext
  
  override def sqlContext() = _hiveContext
  
}
