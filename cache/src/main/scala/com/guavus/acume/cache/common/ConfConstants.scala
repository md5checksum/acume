package com.guavus.acume.cache.common

import scala.collection.mutable.Map
import com.guavus.acume.cache.core.AcumeCacheType

/**
 * @author archit.thakur
 *
 */

private [acume] object ConfConstants {

  val defaultValueMap = Map[String, Any]()
  defaultValueMap += businesscubexml -> "src/test/resources/cubedefinition1.xml"
  defaultValueMap += acumeCacheDefaultType -> AcumeCacheType.acumeStarSchemaTreeCache.name
  val businesscubexml = "acume.cache.baselayer.businesscubexml"
  val schedularinterval = "acume.cache.core.schedularinterval"
  val variableretentionmap = "acume.cache.core.variableretentionmap"
  val instainstanceid = "acume.cache.baselayer.instainstanceid"
  val storagetype = "acume.cache.baselayer.storagetype"
  val timezone = "acume.core.global.timezone"
  val timezonedb = "acume.cache.core.timezonedb"
  val timezonedbPath = "acume.cache.core.timezonedbPath"
  val whichcachetouse = "acume.cache.core.cachename"
  val rrcacheconcurrenylevel = "acume.cache.core.rrcacheconcurrenylevel"
  val rrsize = ("acume.cache.core.rrcahcesize", 3)
  val instabase = "acume.cache.baselayer.instabase"
  val cubedefinitionxml = "acume.cache.baselayer.cubedefinitionxml"
  val qltype = "acume.cache.execute.qltype"
  val rrloader = "acume.cache.rrcache.loader"
  val acumecachesqlcorrector = "acume.cache.sql.corrector"
  val acumecachesqlparser = "acume.cache.sql.parser"
  
  val cubetype = "cubetype"
  val levelpolicymap = "levelpolicymap"
  val binsource = "binsource"
  val basegranularity = "basegranularity"
  val timeserieslevelpolicymap = "timeserieslevelpolicymap"
  val evictionpolicyforcube = "evictionpolicyclass"
  val cachetype = "cacheType"
    
  val acumecorebinsource = "acume.core.global."+binsource
  val acumecorelevelmap = "acume.core.global."+levelpolicymap
  val acumecoretimeserieslevelmap = "acume.core.global."+timeserieslevelpolicymap
  val acumeglobalbasegranularity = "acume.core.global."+basegranularity
  val acumeglobalevictionpolicycube = "acume.core.global."+evictionpolicyforcube
  
//  val firstbinpersistedtime = "acume.cache.delete.firstbinpersistedtime"
//  val lastbinpersistedtime = "acume.cache.delete.lastbinpersistedtime"
  val completelist = "acume.cache.delete.completelist"
  val backendDbName = "acume.cache.backend.dbname"
  val acumeCacheDefaultType = "acume.cache.default.cache.type"
  val acumeCacheSingleEntityCacheSize = "acume.cache.singleentity.cache.size"
}




