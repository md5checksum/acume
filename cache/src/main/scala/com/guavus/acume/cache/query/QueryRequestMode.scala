package com.guavus.acume.cache.query

object QueryRequestMode extends Enumeration {

  val UI = new QueryRequestMode()
  val SCHEDULER = new QueryRequestMode()
  val FILLER = new QueryRequestMode()

  class QueryRequestMode extends Val

  implicit def convertValue(v: Value): QueryRequestMode = v.asInstanceOf[QueryRequestMode]
}
