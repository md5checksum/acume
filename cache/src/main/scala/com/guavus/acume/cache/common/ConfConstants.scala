package com.guavus.acume.cache.common

import scala.collection.mutable.Map
import com.guavus.acume.cache.core.AcumeCacheType

/**
 * @author archit.thakur
 *
 */

private [acume] object ConfConstants {

  val defaultValueMap = Map[String, String]()
  defaultValueMap += businesscubexml -> "src/test/resources/cubedefinition1.xml"
  defaultValueMap += acumeCacheDefaultType -> AcumeCacheType.acumeStarSchemaTreeCache.name
  defaultValueMap += cacheTypeConfigClassName -> "com.guavus.acume.cache.core.AcumeCacheType"
  
  /* Acume cache properties */ 
  val businesscubexml = "acume.cache.baselayer.businesscubexml"
  val variableretentionmap = "acume.cache.core.variableretentionmap"
  val instainstanceid = "acume.cache.baselayer.instainstanceid"
  val storagetype = "acume.cache.baselayer.storagetype"
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
  val completelist = "acume.cache.delete.completelist"
  val backendDbName = "acume.cache.backend.dbname"
  val acumeCacheDefaultType = "acume.cache.default.cache.type"
  val acumeCacheSingleEntityCacheSize = "acume.cache.singleentity.cache.size"
//val firstbinpersistedtime = "acume.cache.delete.firstbinpersistedtime"
//val lastbinpersistedtime = "acume.cache.delete.lastbinpersistedtime"
  val cubetype = "cubetype"
  val levelpolicymap = "levelpolicymap"
  val binsource = "binsource"
  val basegranularity = "basegranularity"
  val timeserieslevelpolicymap = "timeserieslevelpolicymap"
  val evictionpolicyforcube = "evictionpolicyclass"
    
  /* Acume core properties */
  val timezone = "acume.core.global.timezone"
  val acumecorebinsource = "acume.core.global."+binsource
  val acumecorelevelmap = "acume.core.global."+levelpolicymap
  val acumecoretimeserieslevelmap = "acume.core.global."+timeserieslevelpolicymap
  val acumeglobalbasegranularity = "acume.core.global."+basegranularity
  val acumeglobalevictionpolicycube = "acume.core.global."+evictionpolicyforcube
  val appConfig = "acume.core.app.config"                  
  val sqlQueryEngine = "acume.core.sql.query.engine"
  val udfConfigXml = "acume.core.udf.configurationxml"
  val superUser = "acume.super.user"
  val defaultAggInterval = "acume.insta.defaultAggrInterval"
  val springResolver = "acume.resolver"
  val instaComboPoints = "acume.insta.comboPoints"
  val maxQueryLogRecords = "acume.max.query.log.record"
  val enableJDBCServer = "acume.core.enableJDBCServer"
  
  /* Acume Scheduler properties */  
  val prefetchTaskRetryIntervalInMillis = "acume.scheduler.prefetchTaskRetryIntervalInMillis"
  val enableScheduler = "acume.scheduler.enable"
  val schedulerPolicyClass = "acume.core.scheduler.schedulerpolicyclass"
  val queryPoolPolicyClass = "acume.core.querypoolpolicyclass"
  val schedulerVariableRetentionMap = "acume.scheduler.variableRetentionMap"
  val variableRetentionCombinePoints = "acume.scheduler.variableRetentionCombinePoints"
  val queryPrefetchTaskNoOfRetries = "acume.scheduler.queryPrefetchTaskNoOfRetries"
  val maxSegmentDuration = "acume.scheduler.maxSegmentDuration"
  val schedulerCheckInterval = "acume.scheduler.checkInterval"   
  val threadPoolSize = "acume.scheduler.threadPoolSize"
  val cacheTypeConfigClassName = "acume.cache.type.config.classname"
}
