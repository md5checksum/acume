package com.guavus.acume.cache.eviction

import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.core.AcumeCacheType
import com.guavus.acume.cache.core.AcumeCacheType._

/**
 * @author archit.thakur
 * @param conf specifies AcumeCacheConf
 * @param parameter conf is needed because we need to access first/last bin persisted time, 
 * @param it could be removed once the services from insta are available.
 */
abstract class EvictionPolicy(cube: Cube, conf: AcumeCacheConf) {

  def getEvictableCandidate(cache: List[LevelTimestamp]): Option[LevelTimestamp]
}

object EvictionPolicy{
  
  def getEvictionPolicy(cube: Cube, conf: AcumeCacheConf): EvictionPolicy = {
    
    val evictionPolicyClass = cube.evictionPolicyClass
    val newInstance = evictionPolicyClass.getConstructor(classOf[Cube], classOf[AcumeCacheConf]).newInstance(cube, conf)
    newInstance.asInstanceOf[EvictionPolicy]
  }
}
