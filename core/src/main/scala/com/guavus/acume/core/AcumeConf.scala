package com.guavus.acume.core

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory
import org.apache.shiro.config.Ini
import org.apache.shiro.config.Ini.Section
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.PropertyValidator

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
class AcumeConf(loadDefaults: Boolean, fileName : String) extends Cloneable with Serializable {
  
  private val logger = LoggerFactory.getLogger(this.getClass())

  def this(loadDefaults : Boolean) = this(true, null)
  
  /** Create a AcumeConf that loads defaults from system properties and the classpath */
  def this() = this(true)
  
  private val settings = new HashMap[String, String]()
  private var datasources = Array[String]()
  
  // Set the default values
  setDefault

  // load the properties from the systemProperties
  if (loadDefaults) {
    // Load any Acume.* system properties
    for ((k, v) <- System.getProperties.asScala if k.toLowerCase.contains("acume.") || k.toLowerCase.contains("qb.")) {
      settings(k.trim) = v.trim
    }
  }
  
  // Read the acume.ini file. 
  if(fileName != null) {
    val ini : Ini = Ini.fromResourcePath(fileName)
    val sectionNames = ini.getSectionNames
    
    sectionNames.map(sectionName => {
     val section : Section = ini.getSection(sectionName.trim)
     section.entrySet.map(property => {
       if(!property.getValue.trim.equals("")) {
         datasources.+(sectionName.trim)
         val key = AcumeConf.getKeyName(property.getKey, sectionName)
         settings(key) = property.getValue.trim
         System.setProperty(key, property.getValue.trim)
       }
     })
    })
    
	  PropertyValidator.validate(settings)
  }
  
  private def setDefault = {
    set(ConfConstants.schedulerPolicyClass,"com.guavus.acume.core.scheduler.VariableGranularitySchedulerPolicy")
  }
  
  /** Set a configuration variable. */
  private def set(key: String, value: String): AcumeConf = {
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
  
  /** Set a configuration variable. */
  def setLocalProperty(key: String, value: String): AcumeConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value
    this
  }
  
  def getMaxAllowedQueriesPerClassification() : String = {
    get(ConfConstants.maxAllowedQueriesPerClassification, "default:25")
  }
  
  def getQueryPrefetchTaskRetryIntervalInMillis() : Long = {
    getLong(ConfConstants.prefetchTaskRetryIntervalInMillis, 300000)
  }
  
  def getSchedulerThreadPoolSize() : Int = {
    getInt(ConfConstants.schedulerThreadPoolSize, 2)
  }
  
  def getSuperUser() : String = {
    get(AcumeConf.getKeyName(ConfConstants.superUser), "admin")
  }
  
  def getResolver() : String = {
    get(AcumeConf.getKeyName(ConfConstants.springResolver), "com.guavus.acume.core.spring.AcumeResolver")
  }
  
  def getEnableScheduler() : Boolean = {
    getBoolean(AcumeConf.getKeyName(ConfConstants.enableScheduler), true)
  }

  /* Get the timezone of acume */
  def getAcumeTimeZone() : String = {
    get(AcumeConf.getKeyName(ConfConstants.timezone), "GMT")
  }
  
  def getInstaComboPoints() : Int = {
    getInt(AcumeConf.getKeyName(ConfConstants.instaComboPoints), 24)
  }
  
  def getSchedulerVariableRetentionMap() : String = {
    get(AcumeConf.getKeyName(ConfConstants.schedulerVariableRetentionMap), "1h:24")
  }
  
  def getSchedulerVariableRetentionCombinePoints() : Int = {
    getInt(AcumeConf.getKeyName(ConfConstants.variableRetentionCombinePoints), 1)
  }
  
  def getQueryPrefetchTaskNoOfRetries() : Int = {
    getInt(AcumeConf.getKeyName(ConfConstants.queryPrefetchTaskNoOfRetries), 3)
  }
  
  def getSchedulerMaxSegmentDurationCombinePoints() : Int = {
    getInt(AcumeConf.getKeyName(ConfConstants.maxSegmentDuration), 86400)
  }
  
  def getMaxQueryLogRecords(): Int = {
    getInt(AcumeConf.getKeyName(ConfConstants.maxQueryLogRecords), 10)
  }
  
  def getSchedulerCheckInterval(): Int = {
    getInt(AcumeConf.getKeyName(ConfConstants.schedulerCheckInterval), 300)
  }

  def getDisableTotalForAggregateQueries(datasourceInstance: String): Boolean = {
    getBoolean(AcumeConf.getKeyName(ConfConstants.disableTotalForAggregate, datasourceInstance), false)
  }

  def getEnableJDBCServer(): String = {
    get(AcumeConf.getKeyName(ConfConstants.enableJDBCServer), "false")
  }

  def getAppConfig(): String = {
    get(AcumeConf.getKeyName(ConfConstants.appConfig), "com.guavus.acume.core.configuration.AcumeAppConfig")
  }
  
  def getUdfConfigurationxml() :  String = {
    get(AcumeConf.getKeyName(ConfConstants.udfConfigXml), "udfConfiguration.xml")
  }
  
  def getSchedulerPolicyClass() : String = {
    get(AcumeConf.getKeyName(ConfConstants.queryPoolPolicyClass), "com.guavus.acume.core.QueryPoolPolicyImpl")
  }
  
  def getCacheBaseDirectory(datasourceName : String, instanceName: String) = {
    get(ConfConstants.cacheBaseDirectory, "/data/acume")
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String , datasourceInstance : String = null): String = {
    settings.getOrElse(AcumeConf.getKeyName(key, datasourceInstance), throw new NoSuchElementException(key))
  }

  /** Get a parameter as an Option */
  def getOption(key: String, datasourceInstance : String = null): Option[String] = {
    settings.get(AcumeConf.getKeyName(key, datasourceInstance))
  }

  /** Get all parameters as a list of pairs */
  private def getAll: Array[(String, String)] = settings.clone().toArray

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: => Int, datasourceInstance : String = null): Int = {
    getOption(key, datasourceInstance).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: => Long, datasourceInstance : String = null): Long = {
    getOption(key, datasourceInstance).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: => Double, datasourceInstance : String = null): Double = {
    getOption(key, datasourceInstance).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: => Boolean, datasourceInstance : String = null): Boolean = {
    getOption(key, datasourceInstance).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.contains(key)
  
  /** Set multiple parameters together */
  private def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

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
  
  def getDatasources : Array[String] = {
    datasources
  }
}

object AcumeConf {
   val threadLocal = new ThreadLocal[AcumeConf]
   
   def setConf(conf : AcumeConf) {
     threadLocal.set(conf)
   }
   
   def acumeConf() = {
     if(threadLocal.get() == null) {
    	threadLocal.set(new AcumeConf())
    }
    threadLocal.get()
   }
   
   
   def getKeyName(key: String, dataSourceInstance : String = null): String = {
     if(dataSourceInstance == "common" || dataSourceInstance == null)
       key
     else
       dataSourceInstance.trim + "." + key.trim
   }
  
}
