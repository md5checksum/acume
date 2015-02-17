package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf
import scala.collection.mutable.HashMap

/**
 * @author archit.thakur
 * 
 */
trait AcumeCacheContextTrait extends Serializable {

  private [cache] val poolThreadLocal = new ThreadLocal[HashMap[String, Any]]()
  
  private [acume] def getCubeList: List[Cube]
  private [acume] def isDimension(name: String): Boolean 
  private [acume] def getFieldsForCube(name: String): List[String]
  private [acume] def getAggregationFunction(stringname: String) : String
  private [acume] def getDefaultValue(fieldName: String): Any
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube]

  def threadLocal: ThreadLocal[HashMap[String, Any]] = poolThreadLocal
  def acql(sql: String, qltype: String): AcumeCacheResponse
  def acql(sql: String): AcumeCacheResponse
  def cacheConf : AcumeCacheConf
}


