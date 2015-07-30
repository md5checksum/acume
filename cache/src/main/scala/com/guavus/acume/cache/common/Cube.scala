package com.guavus.acume.cache.common

import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.eviction.EvictionPolicy
import com.guavus.acume.cache.core.AcumeCacheType._
import com.guavus.acume.cache.core.AcumeCacheType
import com.guavus.acume.cache.core.Level

/**
 * @author archit.thakur
 *
 */
abstract class CubeTrait(val cubeName: String, val binSource : String, val superDimension: DimensionSet, val superMeasure: MeasureSet, schemaType : AcumeCacheType, val dataSource : String) extends Serializable {
  def getAbsoluteCubeName : String
}
case class BaseCube(override val cubeName: String, binsource: String, dimension: DimensionSet, measure: MeasureSet, baseGran: TimeGranularity, schemaType : AcumeCacheType = null, override val dataSource : String) extends CubeTrait(cubeName, binsource, dimension, measure, schemaType, dataSource) {
   def getAbsoluteCubeName = {
    cubeName + "_"+ binsource 
  }
}

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

case class Cube(override val cubeName: String, binsource: String, val dataSourceName: String, dimension: DimensionSet, measure: MeasureSet, singleEntityKeys : Map[String, String], 
    baseGran: TimeGranularity, isCacheable: Boolean, levelPolicyMap: Map[Level, Int], diskLevelPolicyMap : Map[Level, Int], cacheTimeseriesLevelPolicyMap: Map[Long, Int], 
    evictionPolicyClass: Class[_ <: EvictionPolicy], schemaType : AcumeCacheType, hbaseConfigs : HbaseConfigs , propertyMap: Map[String,String]) 
    extends CubeTrait(cubeName, binsource, dimension, measure, schemaType, dataSourceName) with Equals {
  
  def getAbsoluteCubeName = {
    cubeName + "_"+ binsource + "_" + dataSourceName
  }
  
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
    val prime = 31
    var result: Int = 1
    result = prime * result + (cubeName.hashCode ^ (cubeName.hashCode >>> 32)).toInt
    result = prime * result + (cubeName.hashCode ^ (binsource.hashCode >>> 32)).toInt
    result
  }
  
}

