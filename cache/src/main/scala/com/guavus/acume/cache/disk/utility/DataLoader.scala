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
import java.util.concurrent.ConcurrentHashMap
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

/**
 * @author archit.thakur
 *
 */
abstract class DataLoader(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, acumeCache: AcumeCache[_ >: Any , _ >: Any]) extends Serializable {

  def loadData(keyMap : Map[String, Any], businessCube: Cube, levelTimestamp: LevelTimestamp): SchemaRDD
//  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable, instabase: String, instainstanceid: String): Tuple2[SchemaRDD, String]
  //This should be removed and things like instabase and instanceid should be retrieviable from MetaDataLoader for better code designing.
  
  def getFirstBinPersistedTime(binSource : String) : Long =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getLastBinPersistedTime(binSource : String) : Long =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getBinSourceToIntervalMap(binSource : String) : Map[Long, (Long,Long)] =  {
    throw new NoSuchMethodException("Method not present")
  }

  def getAllBinSourceToIntervalMap(): Map[String,Map[Long, (Long, Long)]] = {
    throw new NoSuchMethodException("Method not present")
  }

}

object DataLoader {
  private val metadataMap = new ConcurrentHashMap[AcumeCache[_ >: Any, _ >: Any], DataLoadedMetadata]

  private[cache] def getMetadata(key: AcumeCache[_ >: Any, _ >: Any]) = metadataMap.get(key)
  private[cache] def putMetadata(key: AcumeCache[_ >: Any, _ >: Any], value: DataLoadedMetadata) = metadataMap.put(key, value)
  private[cache] def getOrElseInsert(key: AcumeCache[_ >: Any, _ >: Any], defaultValue: DataLoadedMetadata): DataLoadedMetadata = {

    if (getMetadata(key) == null) {

      putMetadata(key, defaultValue)
      defaultValue
    } else
      getMetadata(key)
  }

  private[cache] def getOrElseMetadata(key: AcumeCache[_ >: Any,_ >: Any], defaultValue: DataLoadedMetadata): DataLoadedMetadata = {

    if (getMetadata(key) == null)
      defaultValue
    else
      getMetadata(key)
  }

  def getDataLoader[k,v](acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, acumeCache: AcumeCache[k, v]) = {

    val dataloadermap = acumeCacheContext.dataloadermap
    val dataLoaderClass = StorageType.getStorageType(conf.get(ConfConstants.storagetype)).dataClass
    val instance = dataloadermap.get(dataLoaderClass)
    //    if(dataloadermap.contains(dataLoaderClass)) {
    //      dataloadermap.get(dataLoaderClass)
    //    }
    if (instance == null) {
      val loadedClass = Class.forName(dataLoaderClass)
      val newInstance = loadedClass.getConstructor(classOf[AcumeCacheContextTrait], classOf[AcumeCacheConf], classOf[AcumeCache[_ >: Any,_ >: Any]]).newInstance(acumeCacheContext, conf, acumeCache)
      dataloadermap.put(dataLoaderClass, newInstance.asInstanceOf[DataLoader])
      newInstance.asInstanceOf[DataLoader]
    } else {
      {
        instance
      }
    }
  }
}




