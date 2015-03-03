package com.guavus.acume.util

import java.lang.Error
import scala.collection.mutable.HashMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.core.exceptions.ErrorHandler
import com.guavus.acume.cache.common.ConfConstants

case class PropertyValidator

object PropertyValidator {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[PropertyValidator])
  
  def validate(settings : HashMap[String, String]) = {
    if (validateRetentionMap(settings.get(ConfConstants.variableretentionmap).get, ConfConstants.variableretentionmap) 
        && validateRetentionMap(settings.get(ConfConstants.schedulerVariableRetentionMap).get, ConfConstants.schedulerVariableRetentionMap)
        && isNumber(settings.get(ConfConstants.rrcacheconcurrenylevel).get, ConfConstants.rrcacheconcurrenylevel)
	    && isNumber(settings.get(ConfConstants.rrsize._1).get, ConfConstants.rrsize._1)
	    && isNumber(settings.get(ConfConstants.prefetchTaskRetryIntervalInMillis).get, ConfConstants.prefetchTaskRetryIntervalInMillis)
	    && isNumber(settings.get(ConfConstants.threadPoolSize).get, ConfConstants.threadPoolSize)
	    && isNumber(settings.get(ConfConstants.defaultAggInterval).get, ConfConstants.defaultAggInterval)
	    && isNumber(settings.get(ConfConstants.instaComboPoints).get, ConfConstants.instaComboPoints)
	    && isNumber(settings.get(ConfConstants.variableRetentionCombinePoints).get, ConfConstants.variableRetentionCombinePoints)
	    && isNumber(settings.get(ConfConstants.queryPrefetchTaskNoOfRetries).get, ConfConstants.queryPrefetchTaskNoOfRetries)
	    && isNumber(settings.get(ConfConstants.maxSegmentDuration).get, ConfConstants.maxSegmentDuration)
	    && isNumber(settings.get(ConfConstants.maxQueryLogRecords).get, ConfConstants.maxQueryLogRecords)
	    && isNumber(settings.get(ConfConstants.schedulerCheckInterval).get, ConfConstants.schedulerCheckInterval)
	    && isBoolean(settings.get(ConfConstants.enableJDBCServer).get, ConfConstants.enableJDBCServer)
	    && isBoolean(settings.get(ConfConstants.enableScheduler).get, ConfConstants.enableScheduler))
    {
      logger.info("Valid properties")
    } else {
      ErrorHandler.handleError(new Error("Invalid acume properties..."))
    }
  }
  
  def isBoolean(value : String, key : String = "Key") : Boolean = {
    if(!value.toLowerCase().matches("true|false")) {
      logger.error(key + " is not a boolean")
      return false
    }
    true
  }
  
  def isNumber(value : String, key: String = "Key") : Boolean = {
    if(!value.matches("\\d*")) {
      logger.error(key + " is not a number")
      return false
    }
    true
  }
  
  def validateRetentionMap(value : String, key : String = "Key") : Boolean = {
    val entries = value.split(";")
    if(entries.length == 0) {
      logger.error("Length of " + key + " is invalid...")
      return false
    }
    entries.foreach(entry => {
      val subentry = entry.split(":")
      if(subentry.length != 2 || !isNumber(subentry(1), key)) {
        logger.error("Format of " + key + " is invalid...")
        return false
      }
    })
    true
  }
  

}