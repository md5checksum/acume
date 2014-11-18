package com.guavus.acume.cache.disk.utility

import scala.collection.mutable.HashMap

/**
 * @author archit.thakur
 *
 */
object DataLoadedMetadata {
  
  val dimensionSetStartTime = "dimensionSetStartTime"
  val dimensionSetEndTime = "dimensionSetEndTime"
}

class DataLoadedMetadata {

  private val DataLoadedMetaDataMap = new HashMap[String, String]
  
  def get(key: String) = DataLoadedMetaDataMap.get(key)
  def put(key: String, value: String) = DataLoadedMetaDataMap.put(key, value)
  def getOrElse(key: String, defaultValue:String): String = {
    
    get(key) match {
      case None => defaultValue
      case Some(x) => x
    }
  }
}