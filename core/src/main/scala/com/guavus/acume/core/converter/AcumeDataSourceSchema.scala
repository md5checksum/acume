package com.guavus.acume.core.converter

import com.guavus.qb.cube.schema.QueryBuilderSchema
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.qb.cube.schema.ICube
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.AcumeContext
import com.guavus.qb.cube.schema.FieldType
import com.guavus.qb.cube.schema.IField
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
    val cubes  = acumeContext.acc.getCubeList
    cubes.map(cube=> {
    	var dimensions = cube.dimension.dimensionSet.map(field => {
    		new Field(FieldType.DIMENSION, FieldType.DIMENSION, field.getDefaultValue.asInstanceOf[AnyRef],field.getName, "")
    	}).toList
      dimensions = dimensions.filter(!_.getName().equalsIgnoreCase("ts"))
    	dimensions = dimensions ::: List(new Field(FieldType.DIMENSION, FieldType.DIMENSION, new Integer(0), "ts", "")) ::: cube.measure.measureSet.map(field => {
    		new Field(FieldType.MEASURE, FieldType.MEASURE, field.getDefaultValue.asInstanceOf[AnyRef],field.getName, field.getAggregationFunction)}).toList
      new Cube(cube.cubeName, dimensions, cube.binsource, cube.baseGran.getGranularity, cube.dataSourceName, cube.propertyMap)
    })
  }

  /**
   * returns true if dimension
   */
  override def isDimension(fieldName: String): Boolean = {
    try {
    	fieldName.equalsIgnoreCase("ts") || acumeContext.acc.isDimension(fieldName)
    } catch {
      case ex : RuntimeException => false
    }
  }
  
  /**
   * 
   */
  override def getFieldsForCube(cube : ICube) : java.util.List[String] = {
    acumeContext.acc.getFieldsForCube(cube.getCubeName(), cube.getBinSourceValue()).toList
  }
  
  override def getDefaultAggregateFunction(field : String) : String  = {
    acumeContext.acc.getAggregationFunction(field)
  }

  override def getCubeListContainingAllFields(fields : java.util.List[String]) : java.util.List[String] = {
    acumeContext.acc.getCubeListContainingFields(fields.filter(x => !x.equalsIgnoreCase("ts")).toList).map(x=> x.cubeName)
  }
  
  override def getDefaultValueForField(fieldName : String ) : Object = acumeContext.acc.getDefaultValue(fieldName).asInstanceOf[AnyRef]
}