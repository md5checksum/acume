package com.guavus.acume.cache.core

import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList

trait CacheTimeSeriesPolicyTrait extends Serializable {

  def getLevelToUse(startTime: Long, endTime: Long/*, binSource: String, binclass: String*/): Long
  def getAggregationIntervals(/*binSource: String, binclass: String*/): HashMap[MutableList[Long], Long]
  /*def filterMap(baseLevel: Long): Unit*/
  def copy(): CacheTimeSeriesPolicyTrait
  def getAllLevels(): List[Long]
}
