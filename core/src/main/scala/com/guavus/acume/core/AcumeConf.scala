package com.guavus.acume.core

import java.io.InputStream
import java.util.Properties

import scala.Array.canBuildFrom
import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import scala.collection.mutable.HashMap

import org.slf4j.LoggerFactory

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

  if (loadDefaults) {
    // Load any Acume.* system properties
    for ((k, v) <- System.getProperties.asScala if k.toLowerCase.startsWith("acume.") || k.toLowerCase.startsWith("qb.")) {
      settings(k) = v
    }
  }
  
  if(fileName != null) {
    // Load properties from file
	val properties = new Properties()
	properties.load(fileName)
	for ((k, v) <- properties.asScala) {
		settings(k) = v
		System.setProperty(k, v)
	}
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
    this
  }

  /**
   * The current bin time to be set for the job. This is basically the start time of the bin
   */
  def setResolver(resolver : String): AcumeConf = {
    set("Acume.resolver", resolver)
  }
  
  def getResolver() : String = {
    get("Acume.resolver", "com.guavus.acume.core.spring.AcumeResolver")
  }
  
  /**
   * Sets the input paths for the cubes to be used. Format to use is CubeName1:path1;path2|CubeName2:path1;path2
   */
  def setMaxQueryLogRecords(maxQueryLogRecords : Int): AcumeConf = {
    set("Acume.max.query.log.record", maxQueryLogRecords.toString)
  }

  def getMaxQueryLogRecords(): Int = {
    getInt("Acume.max.query.log.record", 10)
  }
  
  /**
   * Sets the outputcubes base path to be used.
   */
  def setOutputCubeBasePath(outputCubesBasePath: String): AcumeConf = {
    set("Acume.output.cubes.basepath", outputCubesBasePath)
  }
  
  def getOutputCubeBasePath(): String = {
    get("Acume.output.cubes.basepath")
  }
  
  /** Some output streams are not exported to insta
   *  This takes the list of comma separated names.
   * */
  def setNotExportedStreams(streams : String): AcumeConf = {
    set("Acume.streams.insta.no.export", streams)
  }

  def getNotExportedStreams(): String = {
    get("Acume.streams.insta.no.export", "")
  }
  
  def getNotExportedStreamsArray = if(getNotExportedStreams == null || getNotExportedStreams.size == 0) 
    new Array[String](0)
    else
    getNotExportedStreams().split(',')

  /** Set the cubes names to be disabled while excuting jobs. This takes the list of comma separated names.
   *  If this property is not set no cube will be disabled
   *  
   */
  def setDisabledCubes(cubesToBeDisabled : String): AcumeConf = {
    set("Acume.cubesToBeDisabled", cubesToBeDisabled)
  }

  def getDisabledCubes(): String = {
    get("Acume.cubesToBeDisabled", "")
  }
  
  def setEnableJDBCServer(enableJDBCFlag : String): AcumeConf = {
    set("acume.core.enableJDBCServer", enableJDBCFlag)
  }
  
  def getEnableJDBCServer(): String = {
    get("acume.core.enableJDBCServer", "false")
  }

  def getAppConfig(): String = {
    get("acume.core.app.config", "com.guavus.acume.core.configuration.AcumeAppConfig")
  }
  
  def setAppConfig(appConfig: String): AcumeConf = {
    set("acume.core.app.config", appConfig)
  }
  
  def getSqlQueryEngine(): String = {
    get("acume.core.sql.query.engine", "acume")
  }
  
  def setSqlQueryEngine(sqlQueryEngine: String): AcumeConf = {
    set("acume.core.sql.query.engine", sqlQueryEngine)
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
