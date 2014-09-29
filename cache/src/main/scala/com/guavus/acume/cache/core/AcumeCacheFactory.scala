package com.guavus.acume.cache.core

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe
import scala.collection.mutable.Map
import com.guavus.acume.cache.core.AcumeCacheType._

object AcumeCacheFactory {

//  def getInstance(cacheIdentifier: CacheIdentifier, timeGranularity: TimeGranularity, cType: CacheType, 
//			isCacheable: Boolean, cacheIdentifierSuffix: String, cube: ICube) {
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
//  }
}