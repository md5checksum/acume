package com.guavus.acume.core.query

object FILE_TYPE extends Enumeration {
  val FILTERS = new FILE_TYPE()
  val RESULTS = new FILE_TYPE()
  val ZIP = new FILE_TYPE()

  class FILE_TYPE extends Val

  implicit def convertValue(v: Value): FILE_TYPE = v.asInstanceOf[FILE_TYPE]
}

