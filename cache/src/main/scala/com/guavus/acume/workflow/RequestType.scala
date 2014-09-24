package com.guavus.acume.workflow

object RequestType extends Enumeration {
  type RequestType = Value
  val Aggregate = Value("Aggregate")
  val Timeseries = Value("Timeseries")
}
