package com.guavus.acume.cache.eviction

import com.guavus.acume.cache.core.AcumeCacheObserver
import com.guavus.acume.cache.core.AcumeCache
import com.google.common.cache.LoadingCache
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.AcumeCacheConf
import scala.collection.JavaConversions._
import java.util.Observable
import java.rmi.NoSuchObjectException
import com.guavus.acume.cache.common.AcumeCacheConf


/**
 * @author archit.thakur
 *
 */

class AcumeCacheEvictionObserver(_$acumeCache: AcumeCache[_ <: Any, _ <: Any]) extends AcumeCacheObserver {
  
  _$acumeCache.newObserverAddition(this)
  
  override val acumeCache = _$acumeCache
  override def update(observable: Observable, arg: Any) = {
    
	val conf = arg.asInstanceOf[AcumeCacheConf]
    val loading = acumeCache.getCacheCollection.asInstanceOf[LoadingCache[LevelTimestamp , String]]
    val _$key = loading.asMap().keySet()
    val _$eviction = EvictionPolicy.getEvictionPolicy(acumeCache.cube, _$acumeCache.acumeCacheContext)
    val memoryEvictable = _$eviction.getMemoryEvictableCandidate(_$key.toList)
    val diskEvictable = _$eviction.getDiskEvictableCandidate(_$key.toList)
    if(memoryEvictable != diskEvictable) {
      //evict memory candidate 
    } else {
    	loading.invalidate(memoryEvictable)
    }
  }
}
