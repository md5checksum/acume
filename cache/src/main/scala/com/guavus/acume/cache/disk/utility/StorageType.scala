package com.guavus.acume.cache.disk.utility

object StorageType extends Enumeration {

  val Text = new StorageType("text", "com.guavus.acume.cache.disk.utility.TextDataLoader", "com.guavus.acume.cache.disk.utility.DummyMetaDataLoader")
  
  val Parquet = new StorageType("parquet", "com.guavus.acume.cache.disk.utility.ParquetDataLoader", "com.guavus.acume.cache.disk.utility.DummyMetaDataLoader")
  
  val Orc = new StorageType("orc", "com.guavus.acume.cache.disk.utility.ORCDataLoader", "com.guavus.acume.cache.disk.utility.DummyMetaDataLoader")
  
  
  def getStorageType(name: String): StorageType = { 
    
    for(actualName <- StorageType.values){
      if(name equals actualName.strid)
        return actualName
    }
    Orc
  }
  
  class StorageType(val strid: String, val dataClass: String, val metaDataClass: String) extends Val

  implicit def convertValue(v: Value): StorageType = v.asInstanceOf[StorageType]
}
