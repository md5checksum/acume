package com.guavus.acume.cache.disk.utility

import scala.collection.mutable.HashMap
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._

/**
 * @author archit.thakur
 *
 */
object DataLoadedMetadata {
  
  val dimensionSetStartTime = "dimensionSetStartTime"
  val dimensionSetEndTime = "dimensionSetEndTime"
}

class DataLoadedMetadata {

  
  private val DataLoadedMetaDataMap = new ConcurrentHashMap[String, String]
  
  def this(map: Map[String, String]) = {
   
    this()
    DataLoadedMetaDataMap.putAll(map)
  }
  def get(key: String) = DataLoadedMetaDataMap.get(key)
  def put(key: String, value: String) = DataLoadedMetaDataMap.put(key, value)
  def getOrElseInsert(key: String, defaultValue:String): String = {
    
    if(get(key) == null) { 
      
      put(key, defaultValue)
      defaultValue
    }
    else
      get(key)
  }
  def getOrElse(key: String, defaultValue:String): String = {
    
    if(get(key) == null)
      defaultValue
    else
      get(key)  
  }
}