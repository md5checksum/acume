package com.guavus.acume.cache.common

import com.guavus.acume.cache.common.FieldType._
import com.guavus.acume.cache.common.DataType._

trait Field extends Serializable {

  def getName: String
  def getDataType: DataType
  def getDefaultValue: Any
  def getFieldType: FieldType
}