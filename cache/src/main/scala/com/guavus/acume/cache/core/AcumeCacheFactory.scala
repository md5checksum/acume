package com.guavus.acume.cache.core

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe
import scala.collection.immutable.SortedMap
import scala.collection.mutable.Map
import com.guavus.acume.cache.core.AcumeCacheType._
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.common.Cube
import java.util.concurrent.ConcurrentHashMap
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.eviction.AcumeCacheEvictionObserver

/**
 * @author archit.thakur
 *
 */
object AcumeCacheFactory {

  val caches = new ConcurrentHashMap[CacheIdentifier, AcumeCache]()
  
  /**
   * Factory method to return AcumeCache instances based on types.
   */
  def getInstance(acumeCacheContext: AcumeCacheContext, acumeCacheConf: AcumeCacheConf, cacheIdentifier: CacheIdentifier, cube: Cube) : AcumeCache = {
    /**
     * Logic to compute Cache goes here
     */
    class abc[v >: AcumeCache] extends java.util.function.Function[CacheIdentifier, v]() {
       def apply(t : CacheIdentifier) = {
        val levelSet = cube.levelPolicyMap.keySet.+(cube.baseGran.getGranularity)
        val levels = levelSet.toArray
        //todo check if the cachelevelpolicy used should be configurable.
        val cacheLevelPolicy = new FixedLevelPolicy(levels, cube.baseGran.getGranularity)
        //todo check which cache to use based on the cube configuration and use reflection to create cache object.
        //todo fill below cahcetimelevelmap from cube.
        //todo check if there is a better way for `SortedMap` creation belw.
        val cacheTimeseriesLevelPolicy = new CacheTimeSeriesLevelPolicy(SortedMap[Long, Int]() ++ cube.cacheTimeseriesLevelPolicyMap)
        val _$instance : AcumeCache = cube.schemaType match {
          case `acumeStarSchemaTreeCache` => {
            new AcumeTreeCache(acumeCacheContext, acumeCacheConf, cube, cacheLevelPolicy, cacheTimeseriesLevelPolicy)
          }
          case `acumeFlatSchemaTreeCache` => {
            new AcumeFlatSchemaTreeCache(acumeCacheContext, acumeCacheConf, cube, cacheLevelPolicy, cacheTimeseriesLevelPolicy)
          }
          case _ => throw new IllegalArgumentException(s"No Cache exist for cache type cube $cube.schemaType")
        }
        _$instance
      }
    }
    val _$instance = caches.computeIfAbsent(cacheIdentifier, new abc[AcumeCache]())
    val acumeCacheEvictionObserver = new AcumeCacheEvictionObserver(_$instance)
    _$instance
  }
}