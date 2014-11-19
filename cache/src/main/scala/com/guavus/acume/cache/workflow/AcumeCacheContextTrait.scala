package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.Cube

/**
 * @author archit.thakur
 * 
 */
trait AcumeCacheContextTrait extends Serializable {

  private [acume] def getCubeList: List[Cube]
  private [acume] def isDimension(name: String): Boolean 
  private [acume] def getFieldsForCube(name: String): List[String]
  private [acume] def getDefaultValue(fieldName: String): Any
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube]
  def acql(sql: String, qltype: String): AcumeCacheResponse
  def acql(sql: String): AcumeCacheResponse
}


