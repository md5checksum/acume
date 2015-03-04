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
    if (validateRetentionMap(settings.get(ConfConstants.variableretentionmap), ConfConstants.variableretentionmap) 
        && validateRetentionMap(settings.get(ConfConstants.schedulerVariableRetentionMap), ConfConstants.schedulerVariableRetentionMap)
        && isNumber(settings.get(ConfConstants.rrcacheconcurrenylevel), ConfConstants.rrcacheconcurrenylevel)
	    && isNumber(settings.get(ConfConstants.rrsize._1), ConfConstants.rrsize._1)
	    && isNumber(settings.get(ConfConstants.prefetchTaskRetryIntervalInMillis), ConfConstants.prefetchTaskRetryIntervalInMillis)
	    && isNumber(settings.get(ConfConstants.threadPoolSize), ConfConstants.threadPoolSize)
	    && isNumber(settings.get(ConfConstants.defaultAggInterval), ConfConstants.defaultAggInterval)
	    && isNumber(settings.get(ConfConstants.instaComboPoints), ConfConstants.instaComboPoints)
	    && isNumber(settings.get(ConfConstants.variableRetentionCombinePoints), ConfConstants.variableRetentionCombinePoints)
	    && isNumber(settings.get(ConfConstants.queryPrefetchTaskNoOfRetries), ConfConstants.queryPrefetchTaskNoOfRetries)
	    && isNumber(settings.get(ConfConstants.maxSegmentDuration), ConfConstants.maxSegmentDuration)
	    && isNumber(settings.get(ConfConstants.maxQueryLogRecords), ConfConstants.maxQueryLogRecords)
	    && isNumber(settings.get(ConfConstants.schedulerCheckInterval), ConfConstants.schedulerCheckInterval)
	    && isBoolean(settings.get(ConfConstants.enableJDBCServer), ConfConstants.enableJDBCServer)
	    && isBoolean(settings.get(ConfConstants.enableScheduler), ConfConstants.enableScheduler))
    {
      logger.info("Valid properties")
    } else {
      ErrorHandler.handleError(new Error("Invalid acume properties...", new RuntimeException("Invalid acume properties...")))
    }
  }
  
  def isBoolean(value : Option[String], key : String = "Key") : Boolean = {
    if(value == None) {
      logger.error(key + " is not configured in acume conf")
      true
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
      true
    }
    if(!value.get.matches("\\d*")) {
      logger.error(key + " is not a number")
      return false
    }
    true
  }
  
  def validateRetentionMap(value : Option[String], key : String = "Key") : Boolean = {
    if(value == None) {
      logger.error(key + " is not configured in acume conf")
      true
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
    })
    true
  }
  

}