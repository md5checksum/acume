package com.guavus.acume.core.cube

import java.util.ArrayList
import java.util.List
import com.guavus.acume.core.converter.Field
import com.guavus.qb.cube.schema.FieldType
import com.guavus.qb.cube.schema.ICube
import com.guavus.qb.cube.schema.IField
import scala.collection.JavaConversions._
import com.guavus.qb.cube.schema.ICube

class cube_APN_SUBCR_TT_APP extends ICube {

  override def getCubeName(): String = {
    "meta_data_dump_apn_subcr_tt_app___default_binsrc___60"
  }

  override def getFields(): List[IField] = {
    val fieldList = new ArrayList[IField]()
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "DC", ""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "APN",""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "TT_APP_CAT",""))
    fieldList.add(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new java.lang.Integer(0), "SUBCR",""))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "DOWN_BYTES","SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "UP_BYTES","SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "FLOW_COUNT","SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "FLOW_DURATION","SUM"))
    fieldList.add(new Field(FieldType.MEASURE, FieldType.MEASURE, new java.lang.Integer(0), "PEAK_FLOW_DUR","SUM"))
    fieldList
  }
  def getBinSourceValue() = "default123"
  def getTimeGranularityValue() = 86400
   def getProperties() = {
     null
  }
  
  def getDatasourceName() = "default"
}
