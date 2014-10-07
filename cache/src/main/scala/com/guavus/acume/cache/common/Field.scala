package com.guavus.acume.cache.common

import com.guavus.acume.cache.common.FieldType._
import com.guavus.acume.cache.common.DataType._

trait Field {

  def getName: String
  def getDataType: DataType
  def getFieldType: FieldType
}