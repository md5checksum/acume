package com.guavus.acume.cache.workflow

object RequestType extends Enumeration {
  type RequestType = Value
  val Aggregate = Value("Aggregate")
  val Timeseries = Value("Timeseries")
}
