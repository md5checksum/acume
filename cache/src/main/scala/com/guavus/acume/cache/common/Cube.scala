package com.guavus.acume.cache.common

import com.guavus.acume.cache.core.AcumeCacheType._
import com.guavus.acume.cache.core.TimeGranularity._

abstract class CubeTrait(val superCubeName: String, val superDimension: DimensionSet, val superMeasure: MeasureSet) extends Serializable 	
case class Cube(cubeName: String, dimension: DimensionSet, measure: CubeMeasureSet, 
    baseGran: TimeGranularity, isCacheable: Boolean, levelPolicyMap: Map[Long, Int], cacheTimeseriesLevelPolicyMap: Map[Long, Int])
    extends CubeTrait(cubeName, dimension, MeasureSet(measure.measureSet.map(_.measure)))
case class BaseCube(cubeName: String, dimension: DimensionSet, measure: MeasureSet) extends CubeTrait(cubeName, dimension, measure)

case class Function(functionClass: String, functionName: String) extends Serializable 
case class DimensionSet(dimensionSet: Set[Dimension]) extends Serializable 
case class MeasureSet(measureSet: Set[Measure]) extends Serializable 
case class CubeMeasureSet(measureSet: Set[CubeMeasure]) extends Serializable



