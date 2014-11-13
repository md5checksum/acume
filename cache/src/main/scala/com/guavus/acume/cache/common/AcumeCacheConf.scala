package com.guavus.acume.cache.common

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory
import scala.Array.canBuildFrom
import java.io.InputStream
import java.util.Properties

/**
 * @author archit.thakur
 * Configuration for a Cache application. Used to set various Cache parameters as key-value pairs.
 *
 * Most of the time, you would create a CacheConf object with `new CacheConf()`, which will load
 * values from any `Cache.*` Java system properties set in your application as well. You can also load properties from a file using constructor new CacheConf(true, fileName), In this case,
 * parameters you set directly on the `CacheConf` object take priority over system properties.
 *
 * For unit tests, you can also call `new CacheConf(false)` to skip loading external settings and
 * get the same configuration no matter what the system properties are.
 *
 * All setter methods in this class support chaining. For example, you can write
 * `new CacheConf().setSome("").setOther("")`.
 *
 * Note that once a CacheConf object is passed to Cache, it is cloned and can no longer be modified
 * by the user. Cache does not support modifying the configuration at runtime.
 *
 * @param loadDefaults whether to also load values from Java system properties
 */

class AcumeCacheConf(loadDefaults: Boolean, file: InputStream) extends Cloneable with Serializable {
  
  val logger = LoggerFactory.getLogger(this.getClass())
  
  /** Create a AcumeCacheConf that loads defaults from system properties and the classpath */
  def this() = this(true, getClass().getResourceAsStream("/acume.cache.properties"))
  
  private val settings = new HashMap[String, String]()

  if (loadDefaults) {
    for ((k, v) <- System.getProperties.asScala if k.toLowerCase.startsWith("acume.cache.")) {
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

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.trim.toLong).getOrElse(defaultValue)
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
  private[acume] def getenv(name: String): String = System.getenv(name)

  /** Checks for illegal or deprecated config settings. Throws an exception for the former. Not
    * idempotent - may mutate this conf object to convert deprecated settings to supported ones. */
  private[acume] def validateSettings() {
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }
}
