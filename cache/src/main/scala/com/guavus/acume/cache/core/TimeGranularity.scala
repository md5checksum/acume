package com.guavus.acume.cache.core

import scala.collection.mutable.Map
import scala.reflect.BeanProperty
import scala.collection.JavaConversions._

object TimeGranularity extends Enumeration {

  val MONTH = new TimeGranularity(30 * 24 * 60 * 60L, "1M")
  val THIRTY_ONE_DAYS = new TimeGranularity(31 * 24 * 60 * 60L, "31d")
  val WEEK = new TimeGranularity(7 * 24 * 60 * 60L, "7d")
  val DAY = new TimeGranularity(24 * 60 * 60L, "1d")
  val TWO_DAYS = new TimeGranularity(2 * 24 * 60 * 60L, "2d")
  val THREE_DAYS = new TimeGranularity(3 * 24 * 60 * 60, "3d")
  val FOUR_HOUR = new TimeGranularity(4 * 60 * 60L, "4h")
  val THREE_HOUR = new TimeGranularity(3 * 60 * 60L, "3h")
  val HOUR = new TimeGranularity(60 * 60L, "1h")
  val FIFTEEN_MINUTE = new TimeGranularity(15 * 60L, "15m")
  val FIVE_MINUTE = new TimeGranularity(5 * 60L, "5m")
  val ONE_MINUTE = new TimeGranularity(60L, "1m")
  val HALF_DAY = new TimeGranularity(12 * 60 * 60L, "12h")

class TimeGranularity (@BeanProperty var granularity: Long, variableRetentionName: String)
      extends Val with Serializable {

    @BeanProperty
    var name: String = _

    getSecondsToTimeGranMap.put(granularity, this)
    getVariableRetentionNameToTimeGranMap.put(variableRetentionName, this)
    getGranToVariableRetentionNameMap.put(granularity, variableRetentionName)

    def setTimeGranularityName(name: String) {
      this.name = name
      getNameToTimeGranMap.put(name, this)
    }

    def getDurationInMinutes(): Long = granularity / ONE_MINUTE.getGranularity
    def getDurationInHour(): Long = granularity / HOUR.getGranularity
    def getDurationInDay(): Long = granularity / DAY.getGranularity
  }

  private var nameToTimeGran: Map[String, TimeGranularity] = _
  private var secondsToTimeGran: Map[Long, TimeGranularity] = _
  private var variableRetentionNameToTimeGran: Map[String, TimeGranularity] = _
  private var granTovariableRetentionName: Map[Long, String] = _

  private def getNameToTimeGranMap(): Map[String, TimeGranularity] = {
    if (nameToTimeGran == null) nameToTimeGran = Map[String, TimeGranularity]()
    nameToTimeGran
  }

  private def getSecondsToTimeGranMap(): Map[Long, TimeGranularity] = {
    if (secondsToTimeGran == null) secondsToTimeGran = Map[Long, TimeGranularity]()
    secondsToTimeGran
  }

  private def getVariableRetentionNameToTimeGranMap(): Map[String, TimeGranularity] = {
    if (variableRetentionNameToTimeGran == null) variableRetentionNameToTimeGran = Map[String, TimeGranularity]()
    variableRetentionNameToTimeGran
  }

  private def getGranToVariableRetentionNameMap(): Map[Long, String] = {
    if (granTovariableRetentionName == null) granTovariableRetentionName = Map[Long, String]()
    granTovariableRetentionName
  }

  def getGranToVariableRetentionName(gran: Long): Option[String] = {
    getGranToVariableRetentionNameMap.get(gran)
  }

  def getTimeGranularity(value: Long): Option[TimeGranularity] = secondsToTimeGran.get(value)

  def getTimeGranularityForVariableRetentionName(name: String): Option[TimeGranularity] = {
    getVariableRetentionNameToTimeGranMap.get(name)
  }

  def getTimeGranularityByName(name: String): Option[TimeGranularity] = nameToTimeGran.get(name)
  
  implicit def convertValue(v: Value): TimeGranularity = v.asInstanceOf[TimeGranularity]
}
