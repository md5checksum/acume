package com.guavus.acume.cache.eviction

import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.AcumeCacheType
import com.guavus.acume.cache.core.AcumeCacheType._

/**
 * @author archit.thakur
 *
 */
trait EvictionPolicy {

  def getEvictableCandidate(cache: List[LevelTimestamp], newPoint: LevelTimestamp): LevelTimestamp
}

object EvictionPolicy{
  
//  def getEvictionPolicy(acumeCacheType: AcumeCacheType): EvictionPolicy = {
    
//    val evictionPolicyClass = acumeCacheType.evictionPolicy
//    val newInstance = evictionPolicyClass.newInstance
//    newInstance.asInstanceOf[EvictionPolicy]
//  }
}
