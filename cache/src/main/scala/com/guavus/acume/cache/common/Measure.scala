package com.guavus.acume.cache.common

import com.guavus.acume.cache.common.DataType.DataType
import com.guavus.acume.cache.common.FieldType.FieldType

/**
 * @author archit.thakur
 *
 */
class Measure(name: String, baseFieldName : String, datatype: DataType, aggregationFunction: String, defaultValue: Any) extends Field(baseFieldName) { 
  
  def getName: String = name
  def getFieldType: FieldType = FieldType.Measure
  def getDefaultValue: Any = defaultValue 	
  def getDataType: DataType = datatype
  def getAggregationFunction = aggregationFunction 	
  
  override def equals(ms: Any): Boolean = {
    if(!ms.isInstanceOf[Measure]) false
    else if(ms.asInstanceOf[Measure].getName == this.getName) true
    else false 	
  }
}
