package com.guavus.acume.cache.common

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory
import scala.Array.canBuildFrom
import java.io.InputStream
import java.util.Properties

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

class AcumeCacheConf(loadSystemPropertyOverDefault: Boolean, file: InputStream) extends Cloneable with Serializable {
  
  val logger = LoggerFactory.getLogger(this.getClass())
  
  /** Create a AcumeCacheConf that loads defaults from system properties and the classpath */
  def this() = this(true, null)
  
  private val settings = new HashMap[String, String]()
  settings++=ConfConstants.defaultValueMap
  setDefault

  if (loadSystemPropertyOverDefault) {
    for ((k, v) <- System.getProperties.asScala if k.toLowerCase.startsWith("acume.")) {
      settings(k) = v.trim
    }
  }
  
  if(file != null) {
    // Load properties from file
    val properties = new Properties()
    properties.load(file)
    for ((k, v) <- properties.asScala) {
      settings(k) = v.trim
    }
  }
  
  def setDefault = {
    set(ConfConstants.rrcacheconcurrenylevel,"3")
    .set(ConfConstants.rrloader, "com.guavus.acume.cache.workflow.RequestResponseCache")
    .set(ConfConstants.acumecachesqlcorrector, "com.guavus.acume.cache.sql.AcumeCacheSQLCorrector")
    .set(ConfConstants.acumecachesqlparser, "com.guavus.acume.cache.sql.AcumeCacheParser")
  }
  
  /** Set a configuration variable. */
  def set(key: String, value: String): AcumeCacheConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value.trim
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): AcumeCacheConf = {
    if (!settings.contains(key)) {
      settings(key) = value.trim
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): AcumeCacheConf = {
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
    getOption(key).map(_.trim.toInt).getOrElse(defaultValue)
  }
  
  def getInt(key: String): Int = {
    getOption(key).map(_.trim.toInt).get
  }
  
  /** Get a parameter as String, falling back to a default if not set */
  def getString(key: String, defaultValue: String): String = {
    get(key, defaultValue)
  }
  
  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.trim.toLong).getOrElse(defaultValue)
  }
   
  /** Get a parameter as a long, throw exception if config not found */
  def getLong(key: String): Long = {
    getOption(key).map(_.toLong).getOrElse(throw new IllegalArgumentException("key " + key +" not defined in configuration"))
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
  override def clone: AcumeCacheConf = {
    new AcumeCacheConf().setAll(settings)
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
  def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }
}
