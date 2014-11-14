package com.guavus.acume.cache.eviction

import com.guavus.acume.cache.common.LevelTimestamp

class AcumeTreeCacheEvictionPolicy extends EvictionPolicy {

  override def getEvictableCandidate(cache: List[LevelTimestamp], newPoint: LevelTimestamp): LevelTimestamp = {
    
    null
  }
}