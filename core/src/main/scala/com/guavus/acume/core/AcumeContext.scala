package com.guavus.acume.core

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.workflow.AcumeCacheContext

/**
 * @author pankaj.arora
 *
 * This will keep the sparkcontext and hive context.
 */
class AcumeContext(val acumeConfiguration: AcumeConf) extends AcumeContextTrait {

  val _hiveContext = AcumeContextTraitUtil.hiveContext
  val acumeCacheContext = new AcumeCacheContext(_hiveContext, new AcumeCacheConf)
  
  override def acc() = acumeCacheContext
  
  override def sqlContext() = _hiveContext
  
}
