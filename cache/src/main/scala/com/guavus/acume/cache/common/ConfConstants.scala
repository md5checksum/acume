package com.guavus.acume.cache.common

import scala.collection.mutable.Map
private [acume] object ConfConstants {

  val defaultValueMap = Map[String, Any]()
  defaultValueMap += 
    businesscubexml -> "src/test/resources/cubedefinition1.xml"
  val businesscubexml = "acume.cache.baselayer.businesscubexml"
  val schedularinterval = "acume.cache.core.schedularinterval"
  val variableretentionmap = "acume.cache.core.variableretentionmap"
  val instainstanceid = "acume.cache.baselayer.instainstanceid"
  val storagetype = "acume.cache.baselayer.storagetype"
  val timezone = "acume.cache.core.timezone"
  val timezonedb = "acume.cache.core.timezonedb"
  val whichcachetouse = "acume.cache.core.cachename"
  val rrcacheconcurrenylevel = "acume.cache.core.rrcacheconcurrenylevel"
  val rrsize = ("acume.cache.core.rrcahcesize", "10000")
  val instabase = "acume.cache.baselayer.instabase"
  val cubedefinitionxml = "acume.cache.baselayer.cubedefinitionxml"
  val qltype = "acume.cache.execute.qltype"
  val rrloader = "acume.cache.rrcache.loader"
  val basegranularity = "basegranularity"
  val cubetype = "cubetype"
  val levelpolicymap = "levelpolicymap"
  val timeserieslevelpolicymap = "timeserieslevelpolicymap"
}




