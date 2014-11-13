package com.guavus.acume.cache.utility

import com.guavus.acume.cache.core.Interval
import com.guavus.acume.cache.query.QueryRequestMode._
import scala.reflect.BeanProperty
import scala.collection.JavaConversions._

/**
 * @author archit.thakur
 * 
 */
class QueryOptionalParam {

  @BeanProperty
  var aggregationPolicyPeak: AnyRef = _

  @BeanProperty
  var timeSeriesGranularity: Long = _

  @BeanProperty
  var queryRequestMode: QueryRequestMode = _

  @BeanProperty
  var persistBinTimeInterval: Interval = _

  private var isAggregateFromTS: Boolean = _

  @BeanProperty
  var sqlQueryTimeout: Int = _

  setAggregateFromTS(false)

  def setAggregateFromTS(b: Boolean) {
    isAggregateFromTS = b
  }

  def getAggregateFromTS(): Boolean = isAggregateFromTS
}
