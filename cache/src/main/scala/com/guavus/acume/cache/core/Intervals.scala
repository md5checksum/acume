package com.guavus.acume.cache.core

import Intervals._
import scala.collection.JavaConversions._

object Intervals {

  def newInvalidIntervals(): Intervals = {
    val interval = new Interval(Int.MinValue, Int.MaxValue)
    new Intervals(Array(interval))
  }
  
  def newValidIntervals(intervals: List[Interval]): Intervals = new Intervals(intervals)

}

private [cache] class Intervals(val intervals: List[Interval]) extends Iterable[Interval] {
  
  var granularity: Int = _
  
  def this(ranges: Array[Interval]) {
    this(ranges.toList)
  }

  override def isEmpty(): Boolean = intervals.isEmpty

  override def size(): Int = intervals.size

  def add(r: Interval): Intervals = new Intervals(intervals ++ List(r))

  def addAll(r: Intervals): Intervals = new Intervals(intervals ++ r.intervals)

  override def iterator(): Iterator[Interval] = intervals.iterator()

  def getGranularity(): Int = granularity

  def setGranularity(granularity: Int) {
    this.granularity = granularity
  }

  override def toString(): String = {
    val builder = new StringBuilder()
    builder.append("Ranges [ranges=")
    builder.append(intervals)
    builder.append(", granularity=")
    builder.append(granularity)
    builder.append("]")
    builder.toString
  }
}
