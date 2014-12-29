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
import com.guavus.acume.core.AcumeContextTrait


/**
 * @author pankaj.arora
 * Data source schema for acume. 
 */
class AcumeDataSourceSchema(acumeContext : AcumeContextTrait) extends QueryBuilderSchema {

  /**
   * return the list of data source cubes.
   */
  override def getCubes(): java.util.List[ICube] = {
    val cubes  = acumeContext.ac.getCubeList
    cubes.map(cube=> {
    	var dimensions = cube.dimension.dimensionSet.map(field => {
    		new Field(FieldType.DIMENSION, FieldType.DIMENSION, field.getDefaultValue.asInstanceOf[AnyRef],field.getName, "")
    	}).toList
    	dimensions = dimensions ::: List(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new Integer(0), "ts", "")) ::: cube.measure.measureSet.map(field => {
    		new Field(FieldType.MEASURE, FieldType.MEASURE, field.getDefaultValue.asInstanceOf[AnyRef],field.getName, field.getAggregationFunction)}).toList
      new Cube(cube.cubeName, dimensions, /**TODO Change this to actual binsource when support provided by cache */"", cube.baseGran.getGranularity)
    })
  }

  /**
   * returns true if dimension
   */
  override def isDimension(fieldName: String): Boolean = {
    try {
    	fieldName.equalsIgnoreCase("ts") || acumeContext.ac.isDimension(fieldName)
    } catch {
      case ex : RuntimeException => false
    }
  }
  
  /**
   * 
   */
  override def getFieldsForCube(cubeName : String) : java.util.List[String] = {
    acumeContext.ac.getFieldsForCube(cubeName).toList
  }
  
  override def getDefaultAggregateFunction(field : String) : String  = {
    acumeContext.ac.getAggregationFunction(field)
  }

  override def getCubeListContainingAllFields(fields : java.util.List[String]) : java.util.List[String] = {
    acumeContext.ac.getCubeListContainingFields(fields.filter(x => !x.equalsIgnoreCase("ts")).toList).map(x=> x.cubeName)
  }
  
  override def getDefaultValueForField(fieldName : String ) : Object = acumeContext.ac.getDefaultValue(fieldName).asInstanceOf[AnyRef]
}