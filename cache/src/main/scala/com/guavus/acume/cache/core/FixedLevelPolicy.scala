package com.guavus.acume.cache.core
import scala.collection.mutable.Map
import scala.collection.mutable.MutableList
import scala.collection.JavaConversions._
import com.guavus.acume.cache.utility.Utility
import scala.math.Ordering.Implicits._

@SerialVersionUID(1L)
/**
 * @author archit.thakur
 *
 */
class FixedLevelPolicy(var levels: Array[Long], baseLevel: Long) extends AbstractCacheLevelPolicy(baseLevel) {

  val levelIndex: Map[Long, Int] = Map[Long, Int]()
  val childParentsMap: Map[Long, MutableList[Long]] = Map[Long, MutableList[Long]]()
  val parentChildMap: Map[Long, Long] = Map[Long, Long]()
  var hasBaseLevel = false

  val allLevel = {
    val list = MutableList(levels.toList:_*)
    if(!list.contains(baseLevel))
      list.+:(baseLevel)
    list.sortWith(_<_)
  }
  
  var i = allLevel.size - 1
  while (i >= 0) {
    val currentLevel = allLevel(i)
    var child = -1L
    if (currentLevel == TimeGranularity.MONTH.getGranularity) {
      var j = i - 1
      var breakCondition = false
      while (j >= 0 
          && !breakCondition) {
        val candidateChild = allLevel(j)
        if (candidateChild <= TimeGranularity.DAY.getGranularity) {
          child = candidateChild
          breakCondition = true
        }
        j -= 1
      }
    } else {
      var j = i - 1
      var breakCondition = false
      while (j >= 0 && !breakCondition) {
        val candidateChild = allLevel(j)
        if (currentLevel % candidateChild == 0) {
          child = candidateChild
          breakCondition = true
        }
        j -= 1
      }
    }
    if (child != -1L) {
      parentChildMap.put(currentLevel, child)
      childParentsMap.get(child) match{
        case Some(list) => list.+=(currentLevel)
        childParentsMap.put(child, list)
        case None => childParentsMap.put(child, MutableList[Long]())
      }
    }
    i -= 1
  }
    
  levels = allLevel.toArray
  var index1 = 0
  for (level <- levels) {
    levelIndex.put(level, index1)
    index1 += 1
  }

  override def getLowerLevel(currentLevel: Long): Long = {
    if (currentLevel == baseLevel) -1
    else{
      val currentIndex = levelIndex.get(currentLevel).get
      if (currentIndex == -1) -1
      else levels(currentIndex - 1)
    }
  }

  override def getParentLevel(currentLevel: Long): Long = {
    val currentIndex = levelIndex.get(currentLevel).get
    if (currentIndex == levels.length - 1) -1
    else levels(currentIndex + 1)
  }

  override def getChildrenLevel(currentLevel: Long): Long = {
    val child = parentChildMap.get(currentLevel)
    child match{
      case Some(childrenLevel) => childrenLevel
      case None => -1
    }
  }

  override def getParentSiblingMap(level: Long, time: Long): Map[Long, MutableList[Long]] = {
    val resultMap = Map[Long, MutableList[Long]]()
    val parents = getAllParentsLevel(level)
    val instance = Utility.newCalendar
    for (parent <- parents) {
      val startTime = Utility.floorFromGranularity(time, parent)
      val endTime = Utility.getNextTimeFromGranularity(startTime, parent, instance)
      val intervals = Utility.getAllIntervals(startTime, endTime, level)
      resultMap.+=(parent->intervals)
    }
    resultMap
  }

  private def getAllParentsLevel(currentLevel: Long): MutableList[Long] = {
    childParentsMap.get(currentLevel) match{
      case Some(list) => list
      case None => MutableList[Long]()
    }
  }

  def getParentHierarchy(currentLevel: Long): MutableList[Long] = {
    val parentHierarchyLevels = MutableList[Long]()
    val parentLevels = getAllParentsLevel(currentLevel)
    parentHierarchyLevels.++=(parentLevels)
    for (parentLevel <- parentLevels) {
      parentHierarchyLevels.++=(getParentHierarchy(parentLevel))
    }
    parentHierarchyLevels
  }
}
