package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.AcumeCacheContext
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf

/**
 * @author archit.thakur
 *
 */
abstract class MetaDataLoader {

  def loadMetaData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: String): SchemaRDD
//  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: String, instabase: String, instainstanceid: String): SchemaRDD
}

object MetaDataLoader {
  def getDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) = {
    
    val dataLoaderClass = StorageType.getStorageType(conf.get(ConfConstants.storagetype)).metaDataClass
    val loadedClass = Class.forName(dataLoaderClass)
    val newInstance = loadedClass.getConstructor(classOf[AcumeCacheContext], classOf[AcumeCacheConf], classOf[Cube]).newInstance(acumeCacheContext, conf, cube)
    newInstance.asInstanceOf[MetaDataLoader]
  }
}
