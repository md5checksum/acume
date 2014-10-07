package com.guavus.acume.cache.common

import com.guavus.acume.cache.common.FieldType._
import com.guavus.acume.cache.common.DataType.DataType

class Dimension(name: String, datatype: DataType) extends Field { 
  
  def getName: String = name
  def getFieldType: FieldType = FieldType.Dimension 
  def getDataType: DataType = datatype
  def getAnnotator = null
  
  override def equals(dim: Any): Boolean = {
    if(!dim.isInstanceOf[Dimension]) false 
    else if(dim.asInstanceOf[Dimension].getName == this.getName) true
    else false
  }
}
