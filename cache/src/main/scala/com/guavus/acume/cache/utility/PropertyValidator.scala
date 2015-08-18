package com.guavus.acume.cache.utility

import java.lang.Error
import scala.collection.mutable.HashMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.ConfConstants
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.acume.cache.core.TimeGranularity

case class PropertyValidator()

object PropertyValidator {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[PropertyValidator])
  
  def validate(settings : HashMap[String, String]) = {
    
    if(isValidProperty(settings, ConfConstants.schedulerVariableRetentionMap, validateRetentionMap) && 
       isValidProperty(settings, ConfConstants.acumecorelevelmap, validateRetentionMap) && 
       isValidProperty(settings, ConfConstants.acumecoretimeserieslevelmap, validateTimeSeriesRetentionMap) && 
       isValidProperty(settings, ConfConstants.rrsize._1, isNumber) && 
       isValidProperty(settings, ConfConstants.prefetchTaskRetryIntervalInMillis, isNumber) && 
       isValidProperty(settings, ConfConstants.schedulerThreadPoolSize, isNumber) && 
       isValidProperty(settings, ConfConstants.instaComboPoints, isNumber) && 
       isValidProperty(settings, ConfConstants.variableRetentionCombinePoints, isNumber) && 
       isValidProperty(settings, ConfConstants.queryPrefetchTaskNoOfRetries, isNumber) && 
       isValidProperty(settings, ConfConstants.maxSegmentDuration, isNumber) &&
       isValidProperty(settings, ConfConstants.maxQueryLogRecords, isNumber) && 
       isValidProperty(settings, ConfConstants.schedulerCheckInterval, isNumber) && 
       isValidProperty(settings, ConfConstants.enableJDBCServer, isBoolean) && 
       isValidProperty(settings, ConfConstants.enableScheduler, isBoolean)
      )
    {
      logger.info("Valid properties")
    } else {
      throw new RuntimeException("Invalid acume properties...")
    }
  }
  
  private def isValidProperty(settings : HashMap[String, String], propertyName: String, validationFunc: (Option[String], String) => Boolean): Boolean = {
    val propertySettings = settings.filter(property => property._1.contains(propertyName))
    if(propertySettings.size > 0) {
      return (propertySettings.map(setting => validationFunc(Option(setting._2), setting._1)).filter(_==false).size == 0)
    }
    true
  }
  
  def isBoolean(value : Option[String], key : String = "Key") : Boolean = {
    if(value == None) {
      logger.error(key + " is not configured in acume conf")
      return false
    }
    if(!value.get.toLowerCase().matches("true|false")) {
      logger.error(key + " is not a boolean")
      return false
    }
    true
  }
  
  def isNumber(value : Option[String], key: String = "Key") : Boolean = {
    if(value == None) {
      logger.error(key + " is not configured in acume conf")
      return false
    }
    if(!value.get.matches("\\d*")) {
      logger.error(key + " is not a number")
      return false
    }
    true
  }
  
  def validateTimeSeriesRetentionMap(value : Option[String], key : String = "Key") : Boolean = {
    if(value == None) {
      logger.error(key + " is not configured in acume conf")
      return false
    }
    val entries = value.get.split(";")
    if(entries.length == 0) {
      logger.error("Length of " + key + " is invalid...")
      return false
    }
    entries.foreach(entry => {
      val subentry = entry.split(":")
      if(subentry.length != 2 || !isNumber(Some(subentry(1)), key)) {
        logger.error("Format of " + key + " is invalid...")
        return false
      }
      if (inValidFormatCheck(subentry(0))) {
         logger.error("Format of " + key + " is invalid...")
         return false
       }
    })
    true
  }
  
  def validateRetentionMap(levelPolicy : Option[String], key : String = "Key") : Boolean = {
    if(levelPolicy == None || levelPolicy.get.trim == "") {
      logger.error(key + " is not configured in acume conf")
      return false
    }
    
    val levelpolicySplits = levelPolicy.get.trim.split("\\|")
    val inMemoryPolicy = levelpolicySplits(0).trim
    val diskPolicy = 
      if(levelpolicySplits.size == 1) {
        inMemoryPolicy
      } else {
        levelpolicySplits(1).trim
      }
    
    if(!Some(diskPolicy).exists(_.trim.nonEmpty) || !Some(inMemoryPolicy).exists(_.trim.nonEmpty)) {
      return false
    }

    if (!splitAndFormatCheck(inMemoryPolicy, key)) {
      logger.error("Format of " + key + " is invalid for memoryPolicy")
      return false
    }
    
    if (diskPolicy != inMemoryPolicy) {
      if (!splitAndFormatCheck(diskPolicy, key)) {
        logger.error("Format of " + key + " is invalid for diskPolicy")
        return false
      }
    }
    val inMemoryPolicyMap = Utility.getLevelPointMap(inMemoryPolicy)
    val diskPolicyMap = Utility.getLevelPointMap(diskPolicy)
    
    // Check whether disPolicyMap is > than inMemoryPolicyMap
    for((inMemoryLevel, inMemoryPoints) <- inMemoryPolicyMap) {
      val diskPolicyPoints = diskPolicyMap.get(inMemoryLevel).getOrElse({
        logger.error("DiskPolicyMap doesnt have all the levels configured in cachelevelPolicyMap")
        return false
      })
      
      val matches = diskPolicyMap.entrySet().filter(level => {level.getKey().level == inMemoryLevel.level && level.getKey().aggregationLevel == inMemoryLevel.aggregationLevel}).size
      if(matches == 0) {
        logger.error("DiskPolicyMap aggregationPoints cannot be less than inMemorylevel aggregation points")
        return false
      }

      if(inMemoryPoints < 0) {
        logger.error("Number of points cannot be less than 0")
        return false
      }
      
      if(diskPolicyPoints < inMemoryPoints) {
        logger.error("DiskPolicyMap cannot be less than inMemorylevelPolicyMap")
        return false
      }
    
      val fraction = inMemoryLevel.aggregationLevel/inMemoryLevel.level
      if(Math.ceil(fraction).toLong != fraction) {
        logger.error("Combining level is not a multiple of base level")
       return false
      }
      
    }
    true
  }
  
  def splitAndFormatCheck(policyMap: String, key: String = "key"): Boolean = {
    val entries = policyMap.split(";")

    if (entries.length == 0) {
      return false
    }

    entries.foreach(entry => {
      val subentry = entry.split(":")
      if (!(subentry.length == 2 || subentry.length == 3) && !isNumber(Some(subentry(1)), key)) {
        return false
      }
      if (subentry.length != 2) {
        if (inValidFormatCheck(subentry(0)) || inValidFormatCheck(subentry(2))) {
          return false
        }
      } else {
        if (inValidFormatCheck(subentry(0))) {
          return false
        }
      }
    })
    return true
  }

  def inValidFormatCheck(subSubEntry: String): Boolean = {
    return (TimeGranularity.getTimeGranularityForVariableRetentionName(subSubEntry) == None)
  }

}