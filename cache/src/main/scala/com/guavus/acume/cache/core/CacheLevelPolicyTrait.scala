package com.guavus.acume.cache.core

import java.io.Serializable
import scala.collection.mutable.MutableList
import scala.collection.mutable.Map
import scala.collection.JavaConversions._

/**
 * @author archit.thakur
 *
 */
trait CacheLevelPolicyTrait extends Serializable {
  var levels : Array[Level]
  def getRequiredIntervals(startTime: Long, endTime: Long): Map[Long, MutableList[Long]]
  def getRequiredIntervals1(startTime: Long, endTime: Long): Map[Long, MutableList[(Long, Long)]]
  def getParentInterval(time: Long, level: Long): Long
  def getChildrenIntervals(startTime: Long, level: Long): MutableList[Long]
  def getBaseLevel(): Long
  def getParentLevel(currentLevel: Long): Long
  def getChildrenLevel(currentLevel: Long): Long
  def getLowerLevel(currentLevel: Long): Long
  def getFloorToLevel(time: Long, level: Long): Long
  def getCeilingToLevel(time: Long, level: Long): Long
  def getParentSiblingMap(level: Long, time: Long): Map[Long, MutableList[Long]]
  def getParentHierarchy(currentLevel: Long): MutableList[Long]
  def getAggregationLevel(currentLevel: Long): (Long/*level*/)
  def getCombinableIntervals(startTime: Long, level: Long, childrenLevel : Long): MutableList[Long]
  
}
