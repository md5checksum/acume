package com.guavus.acume.core.scheduler

import java.util.ArrayList
import java.util.List
import com.guavus.acume.core.converter.Field
import com.guavus.qb.cube.schema.FieldType
import com.guavus.qb.cube.schema.ICube
import com.guavus.qb.cube.schema.IField
import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.acume.core.cube.cube_APN_SUBCR_TT_APP
import com.guavus.acume.core.cube.cube_RAT_DEV_URL
import com.guavus.acume.core.cube.cube_RAT_SEG_DEV_URL
import scala.collection.JavaConversions._

class DummyQueryBuilderSchema extends QueryBuilderSchema {

  override def getCubes(): List[ICube] = {
    val cubeList = new ArrayList[ICube]()
//    cubeList.add(new cube_APN_SUBCR_TT_APP())
    cubeList.add(new cube_RAT_DEV_URL())
//    cubeList.add(new cube_RAT_SEG_DEV_URL())
    cubeList
  }

  override def isDimension(field: String): Boolean = {
    val dimensionList = new ArrayList[String]()
    dimensionList.add("DC")
    dimensionList.add("APN")
    dimensionList.add("TT_APP_CAT")
    dimensionList.add("SUBCR")
    dimensionList.add("RAT")
    dimensionList.add("DEVICE")
    dimensionList.add("URL_CATEGORY")
    dimensionList.add("SP")
    dimensionList.add("APP_TYPE")
    dimensionList.add("SEGMENT")
    if (dimensionList.contains(field.toUpperCase())) {
      true
    } else {
      false
    }
  }

  override def getDefaultAggregateFunction(measure: String): String = "SUM"

  override def getCubeListContainingAllFields(lstfieldNames: List[String]): List[String] = {
    val lstcubeName = new ArrayList[String]()
    for (c <- getCubes) {
      val lstStringCubeFieldNames = new ArrayList[String]()
      for (f <- c.getFields) {
        lstStringCubeFieldNames.add(f.getName)
      }
      if (lstStringCubeFieldNames.containsAll(lstfieldNames)) {
        lstcubeName.add(c.getCubeName)
      }
    }
    lstcubeName
  }

  override def getFieldsForCube(cubename: String): List[String] = {
    val listOfFields =
      if (cubename.toUpperCase() == "RAT_SEG_DEV_URL") {
        (new cube_RAT_SEG_DEV_URL()).getFields
      } else if (cubename.toUpperCase() == "RAT_DEV_URL") {
        (new cube_RAT_DEV_URL()).getFields
      } else {
        (new cube_APN_SUBCR_TT_APP()).getFields
      } 
    val arraylist = new ArrayList[String]()
    for (f <- listOfFields) {
      arraylist.add(f.getName)
    } 
    arraylist
  }

  override def getDefaultValueForField(fieldName: String): AnyRef = new java.lang.Integer(0)
}
