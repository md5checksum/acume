package com.guavus.acume.core.configuration

import java.util.Collection
import java.util.Map
import java.util.Set
import com.guavus.rubix.cache.CacheType
import com.guavus.rubix.cache.Interval
import com.guavus.rubix.cache.Intervals
import com.guavus.rubix.cache.TimeGranularity
import com.guavus.rubix.core.ICube
import com.guavus.rubix.core.IDimension
import com.guavus.rubix.core.IMeasure
import com.guavus.rubix.core.ITransformationContext
import com.guavus.rubix.rules.IRule

trait Config {

  def getBean[T](clazz: Class[T]): T

}