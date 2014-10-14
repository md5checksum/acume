package com.guavus.acume.core.converter

import com.guavus.querybuilder.cube.schema.IField
import com.guavus.querybuilder.cube.schema.FieldType

class Field(fieldType : FieldType, dataType : FieldType, defaultValue : AnyRef, name : String, functionName: String) extends IField {

  def getType() : FieldType = {
    fieldType
  }

	def getDataType() : FieldType = {
	  dataType
	}

	def getDefaultValue() : AnyRef = {
	  defaultValue
	}

	def getName() : String = {
	  name
	}

	def getFunctionName() : String = {
	  functionName
	}

  
}