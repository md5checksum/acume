package com.guavus.acume.cache.eviction

import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.AcumeCacheType
import com.guavus.acume.cache.core.AcumeCacheType._

abstract class EvictionPolicy(conf: AcumeCacheConf) {

  def getEvictableCandidate(cache: List[LevelTimestamp], evictionoptional: Any*): LevelTimestamp
}

object EvictionPolicy{
  
  def getEvictionPolicy(_$eviction: Class[_<:EvictionPolicy], conf: AcumeCacheConf): EvictionPolicy = {
    
    val newInstance = _$eviction.getConstructor(classOf[AcumeCacheConf]).newInstance(conf)
    newInstance.asInstanceOf[EvictionPolicy]
  }
}








