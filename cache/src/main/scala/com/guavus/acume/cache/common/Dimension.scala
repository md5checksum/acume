package com.guavus.acume.cache.common

import com.guavus.acume.cache.common.FieldType._
import com.guavus.acume.cache.common.DataType.DataType

/**
 * @author archit.thakur
 *
 */
class Dimension(name: String, baseFieldName : String, datatype: DataType, defaultValue: Any) extends Field(baseFieldName) { 
  
  def getName: String = name
  def getFieldType: FieldType = FieldType.Dimension 
  def getDataType: DataType = datatype
  def getAnnotator = null
  override def getDefaultValue: Any = defaultValue
   	
  
  
  override def equals(dim: Any): Boolean = {
    if(!dim.isInstanceOf[Dimension]) false 
    else if(dim.asInstanceOf[Dimension].getName == this.getName) true
    else false
  }
}
