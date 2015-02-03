package com.guavus.acume.core.converter

import com.guavus.qb.cube.schema.IField
import com.guavus.qb.cube.schema.FieldType

case class Field(fieldType : FieldType, dataType : FieldType, defaultValue : AnyRef, name : String, functionName: String) extends IField {

	override def getType() = fieldType

	override def getDataType() = dataType

	override def getDefaultValue() = defaultValue

	override def getName() = name

	def getFunctionName() = functionName
  
}