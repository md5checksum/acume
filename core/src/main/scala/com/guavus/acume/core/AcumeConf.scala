package com.guavus.acume.core

import java.io.InputStream
import java.util.Properties

import scala.Array.canBuildFrom
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.HashMap

import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.util.PropertyValidator

/**
 * Configuration for a Acume application. Used to set various Acume parameters as key-value pairs.
 *
 * Most of the time, you would create a AcumeConf object with `new AcumeConf()`, which will load
 * values from any `Acume.*` Java system properties set in your application as well. You can also load properties from a file using constructor new AcumeConf(true, fileName), In this case,
 * parameters you set directly on the `AcumeConf` object take priority over system properties.
 *
 * For unit tests, you can also call `new AcumeConf(false)` to skip loading external settings and
 * get the same configuration no matter what the system properties are.
 *
 * All setter methods in this class support chaining. For example, you can write
 * `new AcumeConf().setSome("").setSome("")`.
 *
 * Note that once a AcumeConf object is passed to Acume, it is cloned and can no longer be modified
 * by the user. Acume does not support modifying the configuration at runtime.
 *
 * @param loadDefaults whether to also load values from Java system properties
 */
class AcumeConf(loadDefaults: Boolean, fileName : InputStream) extends Cloneable with Serializable {
  
  val logger = LoggerFactory.getLogger(this.getClass())

  def this(loadDefaults : Boolean) = this(true, null)
  
  /** Create a AcumeConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new HashMap[String, String]()
  setDefault

  if (loadDefaults) {
    // Load any Acume.* system properties
    for ((k, v) <- System.getProperties.asScala if k.toLowerCase.startsWith("acume.") || k.toLowerCase.startsWith("qb.")) {
      settings(k.trim) = v.trim
    }
  }
  
  if(fileName != null) {
    // Load properties from file
	val properties = new Properties()
	properties.load(fileName)
	for ((k, v) <- properties.asScala) {
		settings(k.trim) = v.trim
		System.setProperty(k.trim, v.trim)
	}
	PropertyValidator.validate(settings)

  }
  
  def setDefault = {
    set(ConfConstants.schedulerpolicyclass,"com.guavus.acume.core.scheduler.VariableGranularitySchedulerPolicy")
  }
  
  /** Set a configuration variable. */
  def set(key: String, value: String): AcumeConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value
    System.setProperty(key, value)
    this
  }

  /**
   * Default Super User in system
   */
  def setQueryPrefetchTaskRetryIntervalInMillis(prefetchTaskRetryIntervalInMillis : String): AcumeConf = {
    set(ConfConstants.prefetchTaskRetryIntervalInMillis, prefetchTaskRetryIntervalInMillis)
  }
  
  def getQueryPrefetchTaskRetryIntervalInMillis() : Long = {
    getLong(ConfConstants.prefetchTaskRetryIntervalInMillis, 300000)
  }
  
  /**
   * Default Super User in system
   */
  def setSchedulerThreadPoolSize(schedulerThreadPoolSize : Int): AcumeConf = {
    set(ConfConstants.threadPoolSize, String.valueOf(schedulerThreadPoolSize))
  }
  
  def getSchedulerThreadPoolSize() : Int = {
    getInt(ConfConstants.threadPoolSize, 2)
  }
  
  /**
   * Default Super User in system
   */
  def setSuperUser(superUser : String): AcumeConf = {
    set(ConfConstants.superUser, superUser)
  }
  
  def getSuperUser() : String = {
    get(ConfConstants.superUser, "admin")
  }
  
  /**
   * Sets the resolver to be used to start app.
   */
  def setDefaultAggrInterval(defaultAggrInterval : String): AcumeConf = {
    set(ConfConstants.defaultAggInterval, defaultAggrInterval)
  }
  
  def getDefaultAggrInterval() : Int = {
    getInt(ConfConstants.defaultAggInterval, -1)
  }
  
  /**
   * Sets the resolver to be used to start app.
   */
  def setResolver(resolver : String): AcumeConf = {
    set(ConfConstants.springResolver, resolver)
  }
  
  def getResolver() : String = {
    get(ConfConstants.springResolver, "com.guavus.acume.core.spring.AcumeResolver")
  }
  
  /**
   * If true it will start scheduler on server startup 
   */
  def setEnableScheduler(enableScheduler : Boolean): AcumeConf = {
    set(ConfConstants.enableScheduler, String.valueOf(enableScheduler))
  }
  
  def getEnableScheduler() : Boolean = {
    getBoolean(ConfConstants.enableScheduler, true)
  }
  
  /**
   * Determines maximum duration query that insta can serve. 
   */
  def setInstaComboPoints(instaComboPoints : Int): AcumeConf = {
    set(ConfConstants.instaComboPoints, String.valueOf(instaComboPoints))
  }
  
  def getInstaComboPoints() : Int = {
    getInt(ConfConstants.instaComboPoints, 24)
  }
  
  /**
   * This tell for which granularity how back scheduler will run 
   */
  def setSchedulerVariableRetentionMap(schedulerVariableRetentionMap : String): AcumeConf = {
    set(ConfConstants.schedulerVariableRetentionMap, schedulerVariableRetentionMap)
  }
  
  def getSchedulerVariableRetentionMap() : String = {
    get(ConfConstants.schedulerVariableRetentionMap, "1h:24")
  }
  
  /**
   * This tells how long queries can be combined together. 
   */
  def setSchedulerVariableRetentionCombinePoints(schedulerVariableRetentionCombinePoints : String): AcumeConf = {
    set(ConfConstants.variableRetentionCombinePoints, schedulerVariableRetentionCombinePoints)
  }
  
  def getSchedulerVariableRetentionCombinePoints() : Int = {
    getInt(ConfConstants.variableRetentionCombinePoints, 1)
  }
  
  /**
   * No of retries before this task mark as failed 
   */
  def setQueryPrefetchTaskNoOfRetries(queryPrefetchTaskNoOfRetries : Int): AcumeConf = {
    set(ConfConstants.queryPrefetchTaskNoOfRetries, String.valueOf(queryPrefetchTaskNoOfRetries))
  }
  
  def getQueryPrefetchTaskNoOfRetries() : Int = {
    getInt(ConfConstants.queryPrefetchTaskNoOfRetries, 3)
  }
  
  /**
   * Scheduler segments which will executw together and increase acume availability 
   */
  def setSchedulerMaxSegmentDuration(schedulerMaxSegmentDuration : Int): AcumeConf = {
    set(ConfConstants.maxSegmentDuration, String.valueOf(schedulerMaxSegmentDuration))
  }
  
  def getSchedulerMaxSegmentDurationCombinePoints() : Int = {
    getInt(ConfConstants.maxSegmentDuration, 86400)
  }
  
  /**
   * Sets the input paths for the cubes to be used. Format to use is CubeName1:path1;path2|CubeName2:path1;path2
   */
  def setMaxQueryLogRecords(maxQueryLogRecords : Int): AcumeConf = {
    set(ConfConstants.maxQueryLogRecords, maxQueryLogRecords.toString)
  }

  def getMaxQueryLogRecords(): Int = {
    getInt(ConfConstants.maxQueryLogRecords, 10)
  }
  
  /**
   * Sets the outputcubes base path to be used.
   */
  def setSchedulerScheckInterval(schedulerCheckInterval : Int): AcumeConf = {
    set(ConfConstants.schedulerCheckInterval, String.valueOf(schedulerCheckInterval))
  }
  
  def getSchedulerCheckInterval(): Int = {
    getInt(ConfConstants.schedulerCheckInterval, 300)
  }

  def setEnableJDBCServer(enableJDBCFlag : String): AcumeConf = {
    set(ConfConstants.enableJDBCServer, enableJDBCFlag)
  }
  
  def getEnableJDBCServer(): String = {
    get(ConfConstants.enableJDBCServer, "false")
  }

  def getAppConfig(): String = {
    get(ConfConstants.appConfig, "com.guavus.acume.core.configuration.AcumeAppConfig")
  }
  
  def setAppConfig(appConfig: String): AcumeConf = {
    set(ConfConstants.appConfig, appConfig)
  }
  
  def getSqlQueryEngine(): String = {
    get(ConfConstants.sqlQueryEngine, "acume")
  }
  
  def setSqlQueryEngine(sqlQueryEngine: String): AcumeConf = {
    set(ConfConstants.sqlQueryEngine, sqlQueryEngine)
  }
  
  def getUdfConfigurationxml() :  String = {
    get(ConfConstants.udfConfigXml, "udfConfiguration.xml")
  }
  
  def getSchedulerPolicyClass() : String = {
    get(ConfConstants.schedulerpolicyclass, "com.guavus.acume.core.QueryPoolPolicyImpl")
  }
  
  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): AcumeConf = {
    if (!settings.contains(key)) {
      settings(key) = value
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): AcumeConf = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    settings.getOrElse(key, defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    settings.get(key)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = settings.clone().toArray

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.contains(key)

  /** Copy this object */
  override def clone: AcumeConf = {
    new AcumeConf(false).setAll(settings)
  }

  /** Checks for illegal or deprecated config settings. Throws an exception for the former. Not
    * idempotent - may mutate this conf object to convert deprecated settings to supported ones. */
  private[AcumeConf] def validateSettings() {
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }
}
