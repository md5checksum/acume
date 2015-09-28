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
abstract class CubeTrait(val superCubeName: String, val superBinSource : String, val superDimension: DimensionSet, val superMeasure: MeasureSet, val superSchemaType : AcumeCacheType, val superDataSource : String) extends Serializable {
  def getAbsoluteCubeName : String = {
    superCubeName + "_"+ superBinSource
  }
}
case class BaseCube(cubeName: String, binSource: String, dimension: DimensionSet, measure: MeasureSet, baseGran: TimeGranularity, schemaType : AcumeCacheType = null, dataSource : String) extends CubeTrait(cubeName, binSource, dimension, measure, schemaType, dataSource) {
}

case class Function(functionClass: String, functionName: String) extends Serializable 

case class DimensionSet(dimensionSet: List[Dimension]) extends Serializable {
  val defaultValueMap = new scala.collection.mutable.HashMap[String, Any]() ++
    dimensionSet.map(x => x.getName -> x.getDefaultValue)
}

case class MeasureSet(measureSet: List[Measure]) extends Serializable {
  val defaultValueMap = new scala.collection.immutable.HashMap[String, Any]() ++
    measureSet.map(x => x.getName -> x.getDefaultValue)
  val measureToAggregateFunction = new scala.collection.immutable.HashMap[String, String]() ++
    measureSet.map(x => x.getName -> x.getAggregationFunction)
}

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

case class Cube(cubeName: String, binSource: String, dataSource: String, dimension: DimensionSet, measure: MeasureSet, singleEntityKeys : Map[String, String], 
    baseGran: TimeGranularity, isCacheable: Boolean, levelPolicyMap: Map[Level, Int], diskLevelPolicyMap : Map[Level, Int], cacheTimeseriesLevelPolicyMap: Map[Long, Int], 
    evictionPolicyClass: Class[_ <: EvictionPolicy], schemaType : AcumeCacheType, hbaseConfigs : HbaseConfigs , propertyMap: Map[String,String]) 
    extends CubeTrait(cubeName, binSource, dimension, measure, schemaType, dataSource) with Equals {
  
  def canEqual(other: Any) = {
    other.isInstanceOf[Cube]
  }

  override def equals(other: Any) = {
    other match {
    case that: Cube => that.canEqual(Cube.this) && cubeName == that.cubeName && binSource == that.binSource
    case _ => false
    }
  }

  override def hashCode() = {
    val prime = 31
    var result: Int = 1
    result = prime * result + (cubeName.hashCode ^ (cubeName.hashCode >>> 32)).toInt
    result = prime * result + (cubeName.hashCode ^ (binSource.hashCode >>> 32)).toInt
    result
  }
  
}

