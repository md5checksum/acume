package com.guavus.acume.cache.common

import com.guavus.acume.cache.common.FieldType._
import com.guavus.acume.cache.common.DataType.DataType

class Measure(name: String, datatype: DataType, aggregationFunction: String, defaultValue: Any) extends Field { 
  
  def getName: String = name
  def getFieldType: FieldType = FieldType.Measure
  def getDefaultValue: Any = defaultValue 	
  def getDataType: DataType = datatype
  def getDefaultAggregationFunction = aggregationFunction 	
  
  override def equals(ms: Any): Boolean = {
    if(!ms.isInstanceOf[Measure]) false
    else if(ms.asInstanceOf[Measure].getName == this.getName) true
    else false 	
  }
}
