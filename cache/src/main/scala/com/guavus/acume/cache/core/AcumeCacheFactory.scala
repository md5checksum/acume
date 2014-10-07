package com.guavus.acume.cache.core

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe
import scala.collection.mutable.Map
import com.guavus.acume.cache.core.AcumeCacheType._
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.common.Cube
import java.util.concurrent.ConcurrentHashMap
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf

object AcumeCacheFactory {

  val caches = new ConcurrentHashMap[CacheIdentifier, AcumeCache]()
  def getInstance(acumeCacheContext: AcumeCacheContext, acumeCacheConf: AcumeCacheConf, cacheIdentifier: CacheIdentifier, cube: Cube) = {
    val instance = caches.get(cacheIdentifier)
    if(instance == null){
      //todo do not use hardcoded array below, use configuration to populate array of level below.
      val levels = Array[Long](300, 3600, 86400, 2592000, 5184000, 10368000)
      //todo check if the cachelevelpolicy used should be configurable.
      val cacheLevelPolicy = new FixedLevelPolicy(levels, cube.baseGran.getGranularity)
      //todo check which cache to use based on the cube configuration and use reflection to create cache object.
      //todo fill below cahcetimelevelmap from cube.
      val cacheTimeseriesLevelPolicy = new CacheTimeSeriesLevelPolicy(null)
      val _$instance = new AcumeTreeCache(acumeCacheContext, acumeCacheConf, cube, cacheLevelPolicy, cacheTimeseriesLevelPolicy)
      caches.put(cacheIdentifier, _$instance)
      _$instance
    }
    else{
      instance
    }
      
//    cacheIdentifier.put("cubeName", value)
//		CacheIdentifier newKey = new CacheIdentifier(cacheIdentifier);
//  def getAcumeCache(name: String, cType: String) = { 
//    
//    public static RubixCache getInstance(CacheIdentifier cacheIdentifier,
//			TimeGranularity timeGranularity, CacheType type, String binSource,
//			Collection<IDimension> dimensionNames,
//			Collection<IMeasure> measureNames, boolean isNonMemoryCache,
//			boolean isCacheable, String cacheIdentifierSuffix, ICube cube,
//			final String parentCacheIdentifier, Set<IDimension> distributedKeys, String binClass, boolean isForcePopulationOn) {
//      
//    }
//    
//    
//    val cacheType = AcumeCacheType.getAcumeCacheType(cType).acumeCache
//    val zx = cacheType.getConstructor(x$1)
  }
}