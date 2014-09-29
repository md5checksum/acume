package com.guavus.acume.cache.core

object AcumeCacheHolder {

//  val acumeDimensionCacheHolder = Map[Long/*timestamp*/, Map[String/*aggregation-level*/, Map[Tuple2[String, String]/*(bin-class, cube)*/, String/*rdd-name*/]]]()
//  val acumeMeasureCacheHolder = Map[Long/*timestamp*/, Map[String/*aggregation-level*/, Map[Tuple2[String, String]/*(bin-class, cube)*/, String/*rdd-name*/]]]()

  
  val acumeDimensionCacheHolder = Map[Long/*timestamp*/, Map[String/*cube*/, String/*rdd-name*/]]()
  val acumeMeasureCacheHolder = Map[Long/*timestamp*/, Map[String/*cube*/, String/*rdd-name*/]]()
}