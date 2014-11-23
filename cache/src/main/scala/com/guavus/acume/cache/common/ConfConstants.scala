package com.guavus.acume.cache.common

import scala.collection.mutable.Map

/**
 * @author archit.thakur
 *
 */

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
  val rrsize = ("acume.cache.core.rrcahcesize", 3)
  val instabase = "acume.cache.baselayer.instabase"
  val cubedefinitionxml = "acume.cache.baselayer.cubedefinitionxml"
  val qltype = "acume.cache.execute.qltype"
  val evictionpolicyforcube = "evictionpolicyclass"
  val rrloader = "acume.cache.rrcache.loader"
  
  val cubetype = "cubetype"
  val levelpolicymap = "levelpolicymap"
  val binsource = "binsource"
  val basegranularity = "basegranularity"
  val timeserieslevelpolicymap = "timeserieslevelpolicymap"
  
  val firstbinpersistedtime = "acume.cache.delete.firstbinpersistedtime"
  val lastbinpersistedtime = "acume.cache.delete.lastbinpersistedtime"
  val completelist = "acume.cache.delete.completelist"
    
}




