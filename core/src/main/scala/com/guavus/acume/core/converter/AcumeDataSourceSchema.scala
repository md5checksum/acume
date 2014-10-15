package com.guavus.acume.core.converter

import com.guavus.querybuilder.cube.schema.QueryBuilderSchema
import scala.collection.JavaConversions._
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
  override def getCubes(): java.util.List[ICube] = {
    val cubes  = acumeContext.ac.getCubeList
    cubes.map(cube=> {
    	var dimensions = cube.dimension.dimensionSet.map(field => {
    		new Field(FieldType.DIMENSION, FieldType.DIMENSION, new Integer(0),field.getName, "")
    	}).toList
    	dimensions = dimensions ::: cube.measure.measureSet.map(field => {
    		new Field(FieldType.MEASURE, FieldType.MEASURE, new Integer(0),field.getName, field.getFunction.functionName)}).toList
      new Cube(cube.cubeName, dimensions)
    })
  }

  /**
   * returns true if dimension
   */
  override def isDimension(fieldName: String): Boolean = {
    acumeContext.ac.isDimension(fieldName)
  }
  
  /**
   * 
   */
  override def getFieldsForCube(cubeName : String) : java.util.List[String] = {
    acumeContext.ac.getFieldsForCube(cubeName).toList
  }
  
  override def getDefaultAggregateFunction(field : String) : String  = {
    acumeContext.ac.getDefaultAggregateFunction(field)
  }

  override def getCubeListContainingAllFields(fields : java.util.List[String]) : java.util.List[String] = {
    acumeContext.ac.getCubeListContainingFields(fields.asScala.toList).map(x=> x.cubeName)
  }
  
  override def getDefaultValueForField(fieldName : String ) : Object = null//acumeContext.ac.getDefaultValueForField(fieldName) 
}