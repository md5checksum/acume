package com.guavus.acume.cache.common

import com.guavus.acume.cache.core.AcumeCacheType._
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.eviction.EvictionPolicy

/**
 * @author archit.thakur
 * 
 */
abstract class CubeTrait(val superCubeName: String, val superDimension: DimensionSet, val superMeasure: MeasureSet) extends Serializable 	
case class Cube(cubeName: String, dimension: DimensionSet, measure: MeasureSet, 
    baseGran: TimeGranularity, isCacheable: Boolean, levelPolicyMap: Map[Long, Int], cacheTimeseriesLevelPolicyMap: Map[Long, Int],
    evictionPolicyClass: Class[_ <: EvictionPolicy])
    extends CubeTrait(cubeName, dimension, measure)
case class BaseCube(cubeName: String, dimension: DimensionSet, measure: MeasureSet) extends CubeTrait(cubeName, dimension, measure)

case class Function(functionClass: String, functionName: String) extends Serializable 
case class DimensionSet(dimensionSet: List[Dimension]) extends Serializable 
case class MeasureSet(measureSet: List[Measure]) extends Serializable 
case class DimensionTable(var tblnm: String) extends Serializable 	

