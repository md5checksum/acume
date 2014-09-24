package com.guavus.acume.util

import com.guavus.acume.cache.Interval
import com.guavus.acume.query.QueryRequestMode
import com.guavus.acume.query.QueryRequestMode._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import scala.collection.JavaConversions._

class QueryOptionalParam {

  @BeanProperty
  var aggregationPolicyPeak: AnyRef = _

  @BeanProperty
  var useBaseGran: Boolean = _

  @BeanProperty
  var timeSeriesGranularity: java.lang.Long = _

  @BeanProperty
  var queryRequestMode: QueryRequestMode = _

  @BeanProperty
  var persistBinTimeInterval: Interval = _

  private var isAggregateFromTS: Boolean = _

  @BeanProperty
  var sqlQueryTimeout: Int = _

  setUseBaseGran(false)

  setAggregateFromTS(false)

  def setAggregateFromTS(b: Boolean) {
    isAggregateFromTS = b
  }

  def getAggregateFromTS(): Boolean = isAggregateFromTS
}