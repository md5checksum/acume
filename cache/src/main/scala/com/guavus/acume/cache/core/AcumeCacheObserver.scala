package com.guavus.acume.cache.core

import java.util.Observer
import com.guavus.acume.cache.common.AcumeCacheConf
import java.util.Observable


abstract class AcumeCacheObserver extends Observer {

  protected val acumeCache: AcumeCache[_ <: Any, _ <: Any]
}




