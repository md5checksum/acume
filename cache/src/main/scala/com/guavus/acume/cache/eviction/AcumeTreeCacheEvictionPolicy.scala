package com.guavus.acume.cache.eviction

import com.guavus.acume.cache.utility.Utility
import java.util.Calendar
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import scala.collection.immutable.TreeMap
import com.guavus.acume.cache.common.LevelTimestamp
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.common.Cube
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.core.Level
import java.util.concurrent.ConcurrentMap
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import org.slf4j.LoggerFactory
import org.slf4j.Logger

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
    if (Utility.getPriority(levelTimestamp.timestamp, levelTimestamp.level.localId, levelTimestamp.aggregationLevel.localId, variableRetentionMap, cacheContext.getLastBinPersistedTime(cube.binsource)) == 0) true else false
  }

  private def intializeMetaData(variableRetentionMap: Map[Long, Int]): HashMap[Long, Long] = {
    val lastBinTime = cacheContext.getLastBinPersistedTime(cube.binsource) //Controller.getInstance.getLastBinPersistedTime(ConfigFactory.getInstance.getBean(classOf[TimeGranularity])
      //.getName, BinSource.getDefault.name(), Controller.RETRY_COUNT)
    val map = HashMap[Long, Long]()
    for ((key, value) <- variableRetentionMap) {
      val numPoints = value
      map.put(key, Utility.getRangeStartTime(lastBinTime, key, numPoints))
    }
    map
  }
}
