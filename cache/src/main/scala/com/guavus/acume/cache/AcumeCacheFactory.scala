package com.guavus.acume.cache

import scala.collection.mutable.Map
import com.guavus.acume.cache.AcumeCacheType
import com.guavus.acume.cache.AcumeCacheType._

object AcumeCacheFactory {

  val nameCacheMap = Map[(AcumeCacheType, String), AcumeCache]()
  def getAcumeCache(name: String, cType: String) = { 
    
    val cacheType = AcumeCacheType.getAcumeCacheType(cType)
    nameCacheMap.get(cacheType, name) match{
      case None => 
      case Some(tuple) => tuple
    }
  }
}