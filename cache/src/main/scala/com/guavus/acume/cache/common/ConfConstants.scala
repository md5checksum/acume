package com.guavus.acume.cache.common

import scala.collection.mutable.Map

import com.guavus.acume.cache.core.AcumeCacheType

/**
 * @author archit.thakur
 *
 */

private [acume] object ConfConstants {

  /* Acume cache properties */ 
  val businesscubexml = "acume.cache.baselayer.businesscubexml"
  val storagetype = "acume.cache.baselayer.storagetype"
  val timezonedbPath = "acume.cache.core.timezonedbPath"
  val rrcacheconcurrenylevel = "acume.cache.core.rrcacheconcurrenylevel"
  val rrsize = ("acume.cache.core.rrcachesize", 3)
  val cubedefinitionxml = "acume.cache.baselayer.cubedefinitionxml"
  val qltype = "acume.cache.execute.qltype"
  val rrloader = "acume.cache.rrcache.loader"
  val acumecachesqlcorrector = "acume.cache.sql.corrector"
  val acumecachesqlparser = "acume.cache.sql.parser"
  val backendDbName = "acume.cache.backend.dbname"
  val acumeCacheDefaultType = "acume.cache.default.cache.type"
  val acumeCacheSingleEntityCacheSize = "acume.cache.singleentity.cache.size"
  
  val levelpolicymap = "levelpolicymap"
  val basegranularity = "basegranularity"
  val timeserieslevelpolicymap = "timeserieslevelpolicymap"
  val evictionpolicyforcube = "evictionpolicyclass"
  val indexDimension = "indexdimension"
  val numberOfPartitions = "numberofpartitions"

  /* Common Properties */
  val superUser = "acume.super.user"
  val springResolver = "acume.resolver"
  val maxQueryLogRecords = "acume.max.query.log.record"
    
  /* Acume core properties */
  val timezone = "acume.core.global.timezone"
  val acumecorebinsource = "acume.core.global.binsource"
  val acumecorelevelmap = "acume.core.global.levelpolicymap"
  val acumecoretimeserieslevelmap = "acume.core.global.timeserieslevelpolicymap"
  val acumeglobalbasegranularity = "acume.core.global.basegranularity"
  val acumeglobalevictionpolicycube = "acume.core.global.evictionpolicyclass"
  val appConfig = "acume.core.app.config"                  
  val sqlQueryEngine = "acume.core.sql.query.engine"
  val udfConfigXml = "acume.core.udf.configurationxml"
  val enableJDBCServer = "acume.core.enableJDBCServer"
  val queryPoolPolicyClass = "acume.core.querypoolpolicyclass"
  val queryPoolSchedPolicyClass = "com.guavus.acume.core.QueryPoolPolicySchedulerImpl"
  val disableTotalForAggregate = "acume.core.disable.total.query"
  val cacheTypeConfigClassName = "acume.cache.type.config.classname"
  
  /* Acume Scheduler properties */  
  val prefetchTaskRetryIntervalInMillis = "acume.scheduler.prefetchTaskRetryIntervalInMillis"
  val enableScheduler = "acume.scheduler.enable"
  val schedulerPolicyClass = "acume.core.scheduler.schedulerpolicyclass"
  val schedulerVariableRetentionMap = "acume.scheduler.variableRetentionMap"
  val variableRetentionCombinePoints = "acume.scheduler.variableRetentionCombinePoints"
  val queryPrefetchTaskNoOfRetries = "acume.scheduler.queryPrefetchTaskNoOfRetries"
  val maxSegmentDuration = "acume.scheduler.maxSegmentDuration"
  val schedulerCheckInterval = "acume.scheduler.checkInterval"   
  val threadPoolSize = "acume.scheduler.threadPoolSize"
  val queryTimeOut = "acume.core.query.timeout"
  val schedulerQueryTimeOut = "acume.scheduler.query.timeout"  
  val maxAllowedQueriesPerClassification = "acume.core.classification.max.allowedQueries" 
  val instaAvailabilityPollInterval = "acume.insta.availability.poll.interval" 
  
  /* Insta Properties */
  val instaComboPoints = "acume.insta.comboPoints"

  val defaultValueMap = Map[String, String]()
  defaultValueMap += businesscubexml -> "src/test/resources/cubedefinition1.xml"
  defaultValueMap += acumeCacheDefaultType -> AcumeCacheType.acumeStarSchemaTreeCache.name
  defaultValueMap += cacheTypeConfigClassName -> "com.guavus.acume.cache.core.AcumeCacheType"
  defaultValueMap += queryTimeOut -> "30"
  defaultValueMap += instaAvailabilityPollInterval -> "300"
  

}
