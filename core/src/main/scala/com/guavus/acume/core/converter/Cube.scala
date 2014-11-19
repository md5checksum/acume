package com.guavus.acume.core.converter

import com.guavus.querybuilder.cube.schema.ICube
import com.guavus.querybuilder.cube.schema.IField
import scala.collection.JavaConversions._

class Cube(cubeName : String, fields : List[IField]) extends ICube {

  def  getFields() : java.util.List[IField] = fields
  
  def getCubeName() = cubeName
  
}