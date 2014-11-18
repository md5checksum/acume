package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.AcumeCacheContext
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.DimensionTable
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.core.AcumeCache

abstract class DataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, acumeCache: AcumeCache) extends Serializable {

  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable): SchemaRDD
  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable, instabase: String, instainstanceid: String): SchemaRDD 
  //This should be removed and things like instabase and instanceid should be retrieviable from MetaDataLoader for better code designing.
}

object DataLoader{
  private val metadataMap = new HashMap[AcumeCache, DataLoadedMetadata]
  
  private [cache] def getMetadata(key: AcumeCache) = metadataMap.get(key)
  private [cache] def putMetadata(key: AcumeCache, value: DataLoadedMetadata) = metadataMap.put(key, value)
  private [cache] def getOrElseMetadata(key: AcumeCache, defaultValue: DataLoadedMetadata): DataLoadedMetadata = {
    
    getMetadata(key) match {
      case None => defaultValue
      case Some(x) => x
    }
  }
  def getDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, acumeCache: AcumeCache) = {
    
    val dataLoaderClass = StorageType.getStorageType(conf.get(ConfConstants.storagetype)).dataClass
    val loadedClass = Class.forName(dataLoaderClass)
    val newInstance = loadedClass.getConstructor(classOf[AcumeCacheContext], classOf[AcumeCacheConf], classOf[AcumeCache]).newInstance(acumeCacheContext, conf, acumeCache)
    newInstance.asInstanceOf[DataLoader]
  }
}
