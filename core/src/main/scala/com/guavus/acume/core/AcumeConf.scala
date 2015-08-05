package com.guavus.acume.core

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory
import org.apache.shiro.config.Ini
import org.apache.shiro.config.Ini.Section
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.PropertyValidator
import java.net.URLClassLoader
import java.util.Map.Entry
import scala.Array.canBuildFrom

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
  
  val settings = new HashMap[String, String]()
  private var datasourceName : String = null
  private var allDatasourceNames : Array[String] = Array[String]()
  
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
    val ini : Ini = Ini.fromResourcePath(ClassLoader.getSystemResource(fileName).getPath)
    val sectionNames = ini.getSectionNames
    
    sectionNames.map(sectionName => {
     val section : Section = ini.getSection(sectionName.trim)
     addDatasourceNames(sectionName)
     section.entrySet.toArray.map(property => {
       val prop = property.asInstanceOf[Entry[String, String]]
       if(!prop.getValue.trim.isEmpty) {
         val key = AcumeConf.getKeyName(prop.getKey, sectionName)
         settings(key) = prop.getValue.trim
         System.setProperty(key, prop.getValue.trim)
       }
     })
    })
//	  PropertyValidator.validate(settings)
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
    System.setProperty(key.trim, value.trim)
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
    getOption(ConfConstants.maxAllowedQueriesPerClassification).getOrElse("default:25")
  }
  
  def getQueryPrefetchTaskRetryIntervalInMillis() : Long = {
    getLong(ConfConstants.prefetchTaskRetryIntervalInMillis).getOrElse(300000)
  }
  
  def getSchedulerThreadPoolSize() : Int = {
    getInt(ConfConstants.schedulerThreadPoolSize).getOrElse(2)
  }
  
  def getSuperUser() : String = {
    getOption(ConfConstants.superUser).getOrElse("admin")
  }
  
  def getResolver() : String = {
    getOption(ConfConstants.springResolver).getOrElse("com.guavus.acume.core.spring.AcumeResolver")
  }
  
  def getEnableScheduler() : Boolean = {
    getBoolean(ConfConstants.enableScheduler).getOrElse(false)
  }

  /* Get the timezone of acume */
  def getAcumeTimeZone() : String = {
    getOption(ConfConstants.timezone).getOrElse("GMT")
  }
  
  def getInstaComboPoints() : Int = {
    getInt(ConfConstants.instaComboPoints).getOrElse(24)
  }
  
  def getSchedulerVariableRetentionMap() : String = {
    getOption(ConfConstants.schedulerVariableRetentionMap).getOrElse("1h:24")
  }
  
  def getSchedulerVariableRetentionCombinePoints() : Int = {
    getInt(ConfConstants.variableRetentionCombinePoints).getOrElse(1)
  }
  
  def getQueryPrefetchTaskNoOfRetries() : Int = {
    getInt(ConfConstants.queryPrefetchTaskNoOfRetries).getOrElse(3)
  }
  
  def getSchedulerMaxSegmentDurationCombinePoints() : Int = {
    getInt(ConfConstants.maxSegmentDuration).getOrElse(86400)
  }
  
  def getMaxQueryLogRecords(): Int = {
    getInt(ConfConstants.maxQueryLogRecords).getOrElse(10)
  }
  
  def getSchedulerCheckInterval(): Int = {
    getInt(ConfConstants.schedulerCheckInterval).getOrElse(300)
  }

  def getDisableTotalForAggregateQueries() : Boolean = {
    getBoolean(ConfConstants.disableTotalForAggregate).getOrElse(false)
  }

  def getEnableJDBCServer(): String = {
    getOption(ConfConstants.enableJDBCServer).getOrElse("false")
  }

  def getAppConfig(): String = {
    getOption(ConfConstants.appConfig).getOrElse("com.guavus.acume.core.configuration.AcumeAppConfig")
  }
  
  def getUdfConfigurationxml() : String = {
    getOption(ConfConstants.udfConfigXml).getOrElse("udfConfiguration.xml")
  }
  
  def getSchedulerPolicyClass() : String = {
    getOption(ConfConstants.queryPoolPolicyClass).getOrElse("com.guavus.acume.core.QueryPoolPolicyImpl")
  }
  
  def getCacheBaseDirectory = {
    getOption(ConfConstants.cacheBaseDirectory).getOrElse("/data/acume")
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter as an Option */
  private def getOption(key: String): Option[String] = {
    val globalFound = settings.get(key)
    globalFound match {
      case None => 
        return settings.get(AcumeConf.getKeyName(key, datasourceName))
      case _ =>
        return globalFound
    }
  }

  /** Get all parameters as a list of pairs */
  private def getAll: Array[(String, String)] = settings.clone().toArray

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String): Option[Int] = {
    getOption(key).map(_.toInt)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String): Option[Long] = {
    getOption(key).map(_.toLong)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String): Option[Double] = {
    getOption(key).map(_.toDouble)
  }
  
  def getBoolean(key: String): Option[Boolean] = {
    getOption(key).map(_.toBoolean)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.contains(key)
  
  /** Set multiple parameters together */
  private def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
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
  
  def getDatasourceName : String = datasourceName
  
  def setDatasourceName(dsName : String) {
    datasourceName = dsName
  }
  
  def getAllDatasourceNames : Array[String] = allDatasourceNames
  
  def addDatasourceNames(dsName: String) {
    if(!dsName.equals("common"))
      allDatasourceNames = allDatasourceNames.+:(dsName)
  }
}

object AcumeConf {
   val threadLocal = new ThreadLocal[AcumeConf]
   
   def setConf(conf : AcumeConf) {
     threadLocal.set(conf)
   }
   
   def acumeConf() : AcumeConf = {
     if(threadLocal.get() == null) {
    	threadLocal.set(AcumeContextTraitUtil.acumeConf)
     }
     threadLocal.get()
   }
   
   def getKeyName(key: String, dataSourceInstance : String = null): String = {
     if(dataSourceInstance == null || dataSourceInstance.toLowerCase.equals("common"))
       key.trim
     else
       dataSourceInstance.trim + "." + key.trim
   }
  
}
