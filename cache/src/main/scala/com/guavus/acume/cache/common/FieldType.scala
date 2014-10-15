package com.guavus.acume.cache.common

object FieldType extends Enumeration {

  val Dimension = new FieldType("dimension")
  val Measure = new FieldType("measure")

  def getFieldType(fieldtype: String): FieldType = {
    for(typeValue <- FieldType.values if(fieldtype == typeValue.typeString))
      return typeValue
    Dimension
  }
  
  class FieldType(val typeString: String) extends Val 
  
  implicit def convertValue(v: Value): FieldType = v.asInstanceOf[FieldType]
}