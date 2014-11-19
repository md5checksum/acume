package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.Cube

/**
 * @author archit.thakur
 * 
 */
trait AcumeCacheContextTrait {

  private [acume] def getCubeList: List[Cube]
  private [acume] def isDimension(name: String): Boolean 
  private [acume] def getFieldsForCube(name: String): List[String]
  private [acume] def getDefaultValue(fieldName: String): Any
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube]
  private [acume] def acql(sql: String, qltype: String): AcumeCacheResponse
  private [acume] def acql(sql: String): AcumeCacheResponse
}


