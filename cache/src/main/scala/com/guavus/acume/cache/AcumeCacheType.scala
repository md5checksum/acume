package com.guavus.acume.cache

object AcumeCacheType extends Enumeration {

  val treeCache = new AcumeCacheType("AcumeTreeCache", classOf[AcumeTreeCache])
  
  def getAcumeCacheType(name: String): AcumeCacheType = { 
    
    for(actualName <- AcumeCacheType.values){
      if(name equals actualName.name)
        return actualName
    }
    treeCache
  }
  class AcumeCacheType(val name: String, val acumeCache: Class[_<:AcumeCache]) extends Val
  
  implicit def convertValue(v: Value): AcumeCacheType = v.asInstanceOf[AcumeCacheType]
}