package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.AcumeCacheContext
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf

abstract class DataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) extends Serializable {

  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: String): SchemaRDD
  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: String, instabase: String, instainstanceid: String): SchemaRDD 
  //This should be removed and things like instabase and instanceid should be retrieviable from MetaDataLoader for better code designing.
}

object DataLoader{
  def getDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) = {
    
    val dataLoaderClass = StorageType.getStorageType(conf.get(ConfConstants.storagetype)).dataClass
    val loadedClass = Class.forName(dataLoaderClass)
    val newInstance = loadedClass.getConstructor(classOf[AcumeCacheContext], classOf[AcumeCacheConf], classOf[Cube]).newInstance(acumeCacheContext, conf, cube)
    newInstance.asInstanceOf[DataLoader]
  }
}
