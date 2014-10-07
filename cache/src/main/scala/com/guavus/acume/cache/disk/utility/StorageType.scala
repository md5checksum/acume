package com.guavus.acume.cache.disk.utility

object StorageType extends Enumeration {

  val Orc = new StorageType("orc", "com.guavus.acume.cache.disk.utility.ORCDataLoader")
//  val Parquet = new StorageType("parquet")
  
  def getStorageType(name: String): StorageType = { 
    
    for(actualName <- StorageType.values){
      if(name equals actualName.strid)
        return actualName
    }
    Orc
  }
  
  class StorageType(val strid: String, val Clx: String) extends Val

  implicit def convertValue(v: Value): StorageType = v.asInstanceOf[StorageType]
}
