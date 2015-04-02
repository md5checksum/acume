package com.guavus.acume.core.converter

import com.guavus.qb.cube.schema.ICube
import com.guavus.qb.cube.schema.IField
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

case class Cube(cubeName: String, fields: List[IField], binSource: String, granularity: Long, properties : Map[String, String]) extends ICube {

  def getFields(): java.util.List[IField] = fields

  def getCubeName() = cubeName

  def getBinSourceValue(): String = {
    binSource
  }

  def getTimeGranularityValue(): java.lang.Long = {
    granularity
  }
  
  def getProperties() : java.util.Map[String, String] = {
	mapAsJavaMap(properties)
  }
}