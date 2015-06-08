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
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import com.guavus.acume.cache.core.AcumeFlatSchemaTreeCache


/**
 * @author archit.thakur
 *
 */

class AcumeCacheEvictionObserver(_$acumeCache: AcumeCache[_ <: Any, _ <: Any]) extends AcumeCacheObserver {

   private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeCacheEvictionObserver])
  
  _$acumeCache.newObserverAddition(this)
  
  override val acumeCache = _$acumeCache
  override def update(observable: Observable, arg: Any) = {
    
	val conf = arg.asInstanceOf[AcumeCacheConf]
    val loading = acumeCache.getCacheCollection.asInstanceOf[LoadingCache[LevelTimestamp , AcumeTreeCacheValue]]
    val _$key = loading.asMap()
    val _$eviction = EvictionPolicy.getEvictionPolicy(acumeCache.cube, _$acumeCache.acumeCacheContext)
    val memoryEvictable = _$eviction.getMemoryEvictableCandidate(_$key.toMap)
    val diskEvictable = _$eviction.getDiskEvictableCandidate(_$key.toMap)
    logger.debug("memory Evictable {} {} , disk evictable {}","", memoryEvictable, diskEvictable)
    if (memoryEvictable != None) {
      if(diskEvictable == None) {
        logger.info("Unpersisting Data object {} for memory", memoryEvictable.get)
        logger.info("Cache Collection {} ", acumeCache.getCacheCollection)
        Some(acumeCache.getCacheCollection.getIfPresent(memoryEvictable.get).asInstanceOf[AcumeTreeCacheValue]).map(_.evictFromMemory)
      } else if(diskEvictable != None) {
        if(memoryEvictable != diskEvictable) {
          logger.info("Unpersisting Data object {} for memory", memoryEvictable.get)
          Some(acumeCache.getCacheCollection.getIfPresent(memoryEvictable.get).asInstanceOf[AcumeTreeCacheValue]).map(_.evictFromMemory)
        }
        logger.info("Unpersisting Data object {} for disk too", memoryEvictable.get)
        loading.invalidate(diskEvictable.get)
      }
    } else if(diskEvictable != None) {
      logger.info("Unpersisting Data object {} for memory_disk", memoryEvictable.get)
      loading.invalidate(diskEvictable.get)
    }
  }
}
