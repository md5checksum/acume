package com.guavus.acume.cache.common

import com.guavus.acume.cache.common.FieldType._
import com.guavus.acume.cache.common.DataType._

/**
 * @author archit.thakur
 *
 */
abstract class Field(baseFieldName : String) extends Serializable {

  def getName: String
  def getBaseFieldName: String = baseFieldName
  def getDataType: DataType
  def getDefaultValue: Any
  def getFieldType: FieldType
}