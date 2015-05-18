package com.guavus.acume.cache.common

//import com.guavus.acume.cache.core.AcumeCacheType._
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.eviction.EvictionPolicy
import com.guavus.acume.cache.core.AcumeCacheType._
import com.guavus.acume.cache.core.AcumeCacheType

/**
 * @author archit.thakur
 *
 */
abstract class CubeTrait(val superCubeName: String, val superDimension: DimensionSet, val superMeasure: MeasureSet, schemaType : AcumeCacheType) extends Serializable
case class BaseCube(cubeName: String, binsource: String, dimension: DimensionSet, measure: MeasureSet, baseGran: TimeGranularity, schemaType : AcumeCacheType = null) extends CubeTrait(cubeName, dimension, measure, schemaType)

case class Function(functionClass: String, functionName: String) extends Serializable 
case class DimensionSet(dimensionSet: List[Dimension]) extends Serializable 
case class MeasureSet(measureSet: List[Measure]) extends Serializable 
case class DimensionTable(var tblnm: String, var maxid: Long) extends Serializable {
  
  def Modify {
    this.synchronized {
      val in = tblnm.indexOf("_")
      if(in == -1)
        tblnm = s"${tblnm}_${System.currentTimeMillis()}"
      else
        tblnm = s"${tblnm.substring(0, in)}_${System.currentTimeMillis()}"
    }
  } 	
}

case class Cube(cubeName: String, binsource: String, dimension: DimensionSet, measure: MeasureSet, singleEntityKeys : Map[String, String], 
    baseGran: TimeGranularity, isCacheable: Boolean, levelPolicyMap: Map[Long, Int], cacheTimeseriesLevelPolicyMap: Map[Long, Int], 
    evictionPolicyClass: Class[_ <: EvictionPolicy], schemaType : AcumeCacheType, propertyMap: Map[String,String]) 
    extends CubeTrait(cubeName, dimension, measure, schemaType) with Equals {
  
  def canEqual(other: Any) = {
    other.isInstanceOf[Cube]
  }

  override def equals(other: Any) = {
    other match {
    case that: Cube => that.canEqual(Cube.this) && cubeName == that.cubeName && binsource == that.binsource
    case _ => false
    }
  }
  
  override def hashCode() = {
      val prime = 41
      prime * (prime + cubeName.hashCode) + binsource.hashCode
    }
}





