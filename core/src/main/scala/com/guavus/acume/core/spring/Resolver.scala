package com.guavus.acume.core.spring

import java.util.Collection
import java.util.Map
import com.guavus.rubix.cache.CacheType
import com.guavus.rubix.cache.Interval
import com.guavus.rubix.cache.Intervals
import com.guavus.rubix.rules.IRule

trait Resolver {

  def getBean[T](clazz: Class[T]): T

}