package com.guavus.acume.cache.core

object AcumeCacheType extends Enumeration {

  val TreeCache = new AcumeCacheType("com.guavus.acume.cache.core.AcumeTreeCache", classOf[AcumeTreeCache])
  
  def getAcumeCacheType(name: String): AcumeCacheType = { 
    
    for(actualName <- AcumeCacheType.values){
      if(name equals actualName.name)
        return actualName
    }
    TreeCache
  }
  class AcumeCacheType(val name: String, val acumeCache: Class[_<:AcumeCache]) extends Val
  implicit def convertValue(v: Value): AcumeCacheType = v.asInstanceOf[AcumeCacheType]
}