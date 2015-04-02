package com.guavus.acume.core.cube

import java.util.ArrayList
import java.util.List
import com.guavus.acume.core.converter.Field
import com.guavus.qb.cube.schema.FieldType
import com.guavus.qb.cube.schema.ICube
import com.guavus.qb.cube.schema.IField
//remove if not needed
import scala.collection.JavaConversions._

class cube_RAT_DEV_URL extends ICube {
  override def getCubeName(): String = {
    "meta_data_dump_rat_dev_url___default_binsrc___60"
  }

  override def getFields(): List[IField] = {
    val fieldList = new ArrayList[IField]()
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "DC", ""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "RAT", ""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "DEVICE", ""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "URL_CATEGORY", ""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "SP", ""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "APP_TYPE", ""))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "DOWN_BYTES", "SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "UP_BYTES", "SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "HIT_COUNT", "SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "HIT_DURATION", "SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "PEAK_HIT_DUR", "SUM"))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Long(0), "ts", ""))
    fieldList
  }
  
  def getBinSourceValue() = "default"
  
  def getTimeGranularityValue() = 3600
  
  def getProperties() = {
     null
  }
  
}
