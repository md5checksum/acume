package com.guavus.acume.cache.common

import scala.collection.mutable.Map

import com.guavus.acume.cache.core.AcumeCacheType

/**
 * @author kashish.jain
 *
 */

private [acume] object ConfConstants {

  /* Acume global properties */
  val superUser = "acume.global.super.user"
  val springResolver = "acume.global.resolver"
  val maxQueryLogRecords = "acume.global.max.query.log.record"
  val businesscubexml = "acume.global.baselayer.businesscubexml"
  val cubedefinitionxml = "acume.global.baselayer.cubedefinitionxml"
  val timezonedbPath = "acume.global.timezonedbPath"
  val timezone = "acume.global.timezone"
  val backendDbName = "acume.global.backend.dbname"
  val enableJDBCServer = "acume.global.enableJDBCServer"
  val appConfig = "acume.global.app.config"
  val acumecorebinsource = "acume.global.binsource"
  val udfConfigXml = "acume.global.udf.configurationxml"
  val acumeglobalbasegranularity = "acume.global.basegranularity"
  val queryTimeOut = "acume.global.query.timeout"
  val maxAllowedQueriesPerClassification = "acume.global.classification.max.allowedQueries"
  val queryPoolPolicyClass = "acume.global.scheduler.querypoolpolicyclass"
  val schedulerPolicyClass = "acume.global.scheduler.schedulerpolicyclass"
  val prefetchTaskRetryIntervalInMillis = "acume.global.scheduler.prefetchTaskRetryIntervalInMillis"
  val schedulerThreadPoolSize = "acume.global.scheduler.threadPoolSize"
  val enableScheduler ="acume.global.scheduler.enable"
  val schedulerVariableRetentionMap = "acume.global.scheduler.variableRetentionMap"
  val variableRetentionCombinePoints = "acume.global.scheduler.variableRetentionCombinePoints"
  val queryPrefetchTaskNoOfRetries = "acume.global.scheduler.queryPrefetchTaskNoOfRetries"
  val maxSegmentDuration = "acume.global.scheduler.maxSegmentDuration"
  val schedulerCheckInterval = "acume.global.scheduler.checkInterval"
  val schedulerQueryTimeOut = "acume.global.scheduler.query.timeout"
  val instaComboPoints = "acume.global.insta.comboPoints"
  val instaAvailabilityPollInterval = "acume.global.insta.availability.poll.interval"
  val cacheBaseDirectory = "acume.global.cache.base.directory"
  val cacheDirectory = "acume.global.cache.directory"
  val queryThreadPoolSize = "acume.global.thread.pool.size"

  /* Cache properties */
  val acumecoretimeserieslevelmap  = "acume.core.global.timeserieslevelpolicymap"
  val disableTotalForAggregate = "acume.core.disable.total.query"
  val rrloader = "acume.cache.rrcache.loader"
  val rrcacheconcurrenylevel = "acume.cache.rrcacheconcurrenylevel"
  val rrsize = ("acume.cache.rrcachesize", 3)
  val storagetype ="acume.cache.baselayer.storagetype"
  val acumeCacheDefaultType = "acume.cache.default.cache.type"
  val acumecachesqlcorrector = "acume.cache.sql.corrector"
  val acumecachesqlparser = "acume.cache.sql.parser"
  val acumeCacheSingleEntityCacheSize = "acume.cache.singleentity.cache.size"
  val cacheTypeConfigClassName = "acume.cache.type.config.classname"
  val acumecorelevelmap = "acume.cache.global.levelpolicymap"
  val acumeEvictionPolicyClass = "acume.cache.global.evictionpolicyclass"

  /* For HIVE. Thin client properties */
  val useInsta = "acume.core.use.insta"
  val firstBinPersistedTime = "acume.cache.delete.firstbinpersistedtime"
  val lastBinPersistedTime = "acume.cache.delete.lastbinpersistedtime"
  
  val levelpolicymap = "levelpolicymap"
  val basegranularity = "basegranularity"
  val timeserieslevelpolicymap = "timeserieslevelpolicymap"
  val evictionpolicyforcube = "evictionpolicyclass"
  val indexDimension = "indexdimension"
  val numberOfPartitions = "numberofpartitions"

  val primaryKeys = "primaryKeys"
  val tableName = "tableName"
  val columnMappings = "columnMappings"

  val queryPoolSchedPolicyClass = "com.guavus.acume.core.QueryPoolPolicySchedulerImpl"
  
  val defaultValueMap = Map[String, String]()
  defaultValueMap += businesscubexml -> "src/test/resources/cubedefinition1.xml"
  defaultValueMap += acumeCacheDefaultType -> AcumeCacheType.acumeFlatSchemaTreeCache.name
  defaultValueMap += cacheTypeConfigClassName -> "com.guavus.acume.cache.core.AcumeCacheType"
  defaultValueMap += queryTimeOut -> "30"
  defaultValueMap += instaAvailabilityPollInterval -> "300"
  defaultValueMap += queryThreadPoolSize -> "16"
  defaultValueMap += useInsta -> "false"

}
