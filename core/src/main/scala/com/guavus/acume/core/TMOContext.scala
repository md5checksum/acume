package com.guavus.acume.core

import com.guavus.acume.cache.workflow.TMOCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf

class TMOContext(override val acumeConfiguration: AcumeConf) extends AcumeContext(acumeConfiguration) {

  override val acumeContext = {
    new TMOCacheContext(hc, new AcumeCacheConf)
  }
  
}