package com.guavus.Cache.core

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import org.slf4j.LoggerFactory

/**
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
class CacheConf(loadDefaults: Boolean) extends Cloneable with Serializable {
  
  val logger = LoggerFactory.getLogger(this.getClass())

  
  /** Create a CacheConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new HashMap[String, String]()

  if (loadDefaults) {
    // Load any Cache.* system properties
    for ((k, v) <- System.getProperties.asScala if k.toLowerCase.startsWith("cache.")) {
      settings(k) = v
    }
  }
  
  /** Set a configuration variable. */
  def set(key: String, value: String): CacheConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings ++= settings
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): CacheConf = {
    if (!settings.contains(key)) {
      settings(key) = value
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): CacheConf = {
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
  override def clone: CacheConf = {
    new CacheConf(false).setAll(settings)
  }

  /** Checks for illegal or deprecated config settings. Throws an exception for the former. Not
    * idempotent - may mutate this conf object to convert deprecated settings to supported ones. */
  private[Cache] def validateSettings() {
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }
}
