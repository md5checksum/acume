package com.guavus.acume.core.converter

import com.guavus.querybuilder.cube.schema.QueryBuilderSchema
import scala.collection.JavaConverters._
import com.guavus.querybuilder.cube.schema.ICube
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.AcumeContext
import com.guavus.querybuilder.cube.schema.FieldType
import com.guavus.querybuilder.cube.schema.IField
import scala.collection.mutable.ArrayBuffer


/**
 * @author pankaj.arora
 * Data source schema for acume. 
 */
class AcumeDataSourceSchema(acumeContext : AcumeContext) extends QueryBuilderSchema {

  /**
   * return the list of data source cubes.
   */
  def getCubes(): List[ICube] = {
    val cubes  = acumeContext.ac.getCubeList
    cubes.map(cube=> {
    	var dimensions = cube.dimension.dimensionSet.map(field => {
    		new Field(FieldType.DIMENSION, FieldType.DIMENSION, 0,field.getName, "")
    	}).toList
    	dimensions = dimensions ::: cube.measure.measureSet.map(field => {
    		new Field(FieldType.MEASURE, FieldType.MEASURE, 0,field.getName, field.getFunction.functionName)}).toList
      new Cube(dimensions)
    })
  }

  /**
   * returns true if dimension
   */
  def isDimension(fieldName: String): Boolean = {
    acumeContext.ac.isDimension(fieldName)
  }
  
  /**
   * 
   */
  def getFieldsForCube(cubeName : String) : List[String] = {
    acumeContext.ac.getFieldsForCube(cubeName).toList
  }
  
  def getDefaultAggregateFunction(field : String) : String  = {
    acumeContext.ac.getDefaultAggregateFunction(field)
  }

  def getCubeListContainingAllFields(fields : List[String]) : List[String] = {
    acumeContext.ac.getCubeListContainingFields(fields).map(x=> x.cubeName)
  }
  
  def getDefaultValueForField(fieldName : String ) : Object = acumeContext.ac.getDefaultValueForField(fieldName) 
}