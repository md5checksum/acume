package com.guavus.acume.core.scheduler

import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.acume.cache.core.Interval

class PrefetchLastCacheUpdateTimeAndInterval {

  private var cacheEndTime: Long = 0

  @BeanProperty
  var cacheEndTimeMap: scala.collection.mutable.HashMap[Long, Long] = _

  @BeanProperty
  var intervals: scala.collection.mutable.HashSet[Interval] = new scala.collection.mutable.HashSet[Interval]()

  def getCacheLastUpdateTime(): Long = this.cacheEndTime

  def setCacheLastUpdateTime(time: Long) {
    this.cacheEndTime = time
  }
}