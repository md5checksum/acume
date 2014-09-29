package com.guavus.acume.cache.disk.schema

import DataType._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

class SchemaDefinition(val list: List[(String, DataType)])

//abstract class DefinitionTrait(acumeCube: AcumeCubes) {
//	
////  def Init(file: String): Unit
//  
////  def getMeasureSchema(cubeId: String): SchemaDefinition
////  def getDimensionSchema(cubeId: String): SchemaDefinition
//  
//  def getSchema(cubeId: String): SchemaDefinition
//  def get(sc: SparkContext, file: String): RDD[Any]
////  def DeInit(file: String): Unit
//  
//}