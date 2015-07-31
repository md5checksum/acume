package com.guavus.acume.cache.common

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory
import org.apache.shiro.config.Ini
import org.apache.shiro.config.Ini.Section

/**
 * @author archit.thakur
 * 
 * Configuration for a Cache application. Used to set various Cache parameters as key-value pairs.
 *
 * Most of the time, you would create AcumeCacheConf object with `new AcumeCacheConf()`, which will load
 * values from any `acume.cache.*` Java system properties set in your application as well. You can also load properties from a file using constructor new CacheConf(true, fileName), In this case,
 * parameters you set directly on the `AcumeCacheConf` object take priority over system properties.
 *
 * For unit tests, you can also call `new AcumeCacheConf(false)` to skip loading external settings and
 * get the same configuration no matter what the system properties are.
 *
 * All setter methods in this class support chaining. For example, you can write
 * `new AcumeCacheConf().setSome("").setOther("")`.
 *
 * Note that once a AcumeCacheConf object is passed to AcumeCacheContext, it is cloned and can no longer be modified
 * by the user. Acume Cache does not support modifying the configuration at runtime.
 *
 * @param loadDefaults whether to also load values from Java system properties
 */

class AcumeCacheConf(loadSystemPropertyOverDefault: Boolean, file: String, var datasourceName: String = null) extends Cloneable with Serializable {
  
  private val logger = LoggerFactory.getLogger(this.getClass())
  
  /** Create a AcumeCacheConf that loads defaults from system properties and the classpath */
  def this() = this(true, null)
  
  private var settings = new HashMap[String, String]()
  
  settings++=ConfConstants.defaultValueMap
  
  // Set the default values
  setDefault
  
  if (loadSystemPropertyOverDefault) {
    for ((k, v) <- System.getProperties.asScala if k.toLowerCase.contains("acume.")) {
      settings(k) = v.trim
    }
  }
  
   // Read the acume.ini file. 
  if(file != null) {
    val ini : Ini = Ini.fromResourcePath(file)
    val sectionNames = ini.getSectionNames
    
    sectionNames.map(sectionName => {
     val section : Section = ini.getSection(sectionName.trim)
     section.entrySet.map(property => {
       if(!property.getValue.trim.equals("")) {
         val key = AcumeCacheConf.getKeyName(property.getKey, sectionName)
         settings(key) = property.getValue.trim
         System.setProperty(key, property.getValue.trim)
       }
     })
    })
  }
  
  private def setDefault = {
    set(ConfConstants.rrcacheconcurrenylevel,"3")
    .set(ConfConstants.rrsize._1, ConfConstants.rrsize._2.toString)
    .set(ConfConstants.rrloader, "com.guavus.acume.cache.workflow.RequestResponseCache")
    .set(ConfConstants.acumecachesqlcorrector, "com.guavus.acume.cache.sql.AcumeCacheSQLCorrector")
    .set(ConfConstants.acumecachesqlparser, "com.guavus.acume.cache.sql.AcumeCacheParser")
  }
  
  /** Set a configuration variable. */
  private def set(key: String, value: String): AcumeCacheConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value.trim
    System.setProperty(key.trim, value.trim)
    this
  }

  /** Set multiple parameters together */
  private def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    settings.get(key).getOrElse(
        settings.get(AcumeCacheConf.getKeyName(key, datasourceName))
    ).asInstanceOf[Option[String]]
  }

  /** Get all parameters as a list of pairs */
  private def getAll: Array[(String, String)] = settings.clone().toArray

  def getInt(key: String): Option[Int] = {
    getOption(key).map(_.trim.toInt)
  }
  
  /** Get a parameter as a long, throw exception if config not found */
  def getLong(key: String): Option[Long] = {
    getOption(key).map(_.trim.toLong)
  }

  /** Get a parameter as a double */
  def getDouble(key: String): Option[Double] = {
    getOption(key).map(_.trim.toDouble)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String): Option[Boolean] = {
    getOption(key).map(_.trim.toBoolean)
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  private [acume] def getenv(name: String): String = System.getenv(name)

  /** Checks for illegal or deprecated config settings. Throws an exception for the former. Not
    * idempotent - may mutate this conf object to convert deprecated settings to supported ones. */
  
  private [acume] def validateSettings(): Boolean = { 
    true
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  private def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }
  
}

object AcumeCacheConf {
  
  def getKeyName(key: String, dataSourceInstance : String = null): String = {
    if(dataSourceInstance == null || dataSourceInstance == "common")
      key.trim
    else
       dataSourceInstance.trim + "." + key.trim
  }
}
