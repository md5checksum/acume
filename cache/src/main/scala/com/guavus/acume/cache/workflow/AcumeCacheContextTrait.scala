package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf

/**
 * @author archit.thakur
 * 
 */
trait AcumeCacheContextTrait extends Serializable {

  private [acume] def getCubeList: List[Cube]
  def isDimension(name: String): Boolean 
  private [acume] def getFieldsForCube(name: String): List[String]
  private [acume] def getAggregationFunction(stringname: String) : String
  def getDefaultValue(fieldName: String): Any
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube]
  def acql(sql: String, qltype: String): AcumeCacheResponse
  def acql(sql: String): AcumeCacheResponse
  private [acume] def cacheConf : AcumeCacheConf
}


