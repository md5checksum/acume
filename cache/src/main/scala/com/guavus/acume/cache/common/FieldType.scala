package com.guavus.acume.cache.common

object FieldType extends Enumeration {

  val Dimension = new FieldType("Dimension")
  val Measure = new FieldType("Measure")

  def getFieldType(fieldtype: String): FieldType = {
    for(typeValue <- FieldType.values if(fieldtype == typeValue.typeString))
      return typeValue
    Dimension
  }
  
  class FieldType(val typeString: String) extends Val 
  
  implicit def convertValue(v: Value): FieldType = v.asInstanceOf[FieldType]
}