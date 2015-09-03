package com.guavus.acume.cache.eviction

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import com.guavus.acume.cache.core.Level
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

/**
 * @author archit.thakur
 *
 */

class AcumeTreeCacheEvictionPolicy(cube: Cube, cacheContext : AcumeCacheContextTrait) extends EvictionPolicy(cube, cacheContext) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeTreeCacheEvictionPolicy])
  
  def getMemoryEvictableCandidate(list: Map[LevelTimestamp, AcumeTreeCacheValue]): Option[LevelTimestamp] = {
    getEvictableCandidate(list.filter(_._2.isInMemory), cube.levelPolicyMap)
  }
  
  def getDiskEvictableCandidate(list: Map[LevelTimestamp, AcumeTreeCacheValue]): Option[LevelTimestamp] = {
    getEvictableCandidate(list, cube.diskLevelPolicyMap)
  }
  
  def getEvictableCandidate(list: Map[LevelTimestamp, AcumeTreeCacheValue], variableretentionmap : Map[Level, Int]): Option[LevelTimestamp] = {
    
    var count = 0
    var _$evictableCandidate: Option[LevelTimestamp] = None
    for((leveltimestamp, _x) <- list) yield {
      
      if(isEvictiable(leveltimestamp, variableretentionmap)) {
        if(count == 0)
          _$evictableCandidate = Some(leveltimestamp)
        count += 1
      }
    }
    if(count > 1)
      logger.warn("WARNING: More than one evictable candidate found.")
    _$evictableCandidate
  }
  
  private def isEvictiable(levelTimestamp: LevelTimestamp, variableRetentionMap: Map[Level, Int]): Boolean = {
    if (Utility.getPriority(levelTimestamp.timestamp, levelTimestamp.level.localId, levelTimestamp.aggregationLevel.localId, variableRetentionMap, BinAvailabilityPoller.getLastBinPersistedTime(cube.binSource)) == 0) true else false
  }

}
