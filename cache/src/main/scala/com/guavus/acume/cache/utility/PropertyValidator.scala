package com.guavus.acume.cache.utility

import java.lang.Error
import scala.collection.mutable.HashMap
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.ConfConstants

case class PropertyValidator
object PropertyValidator {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[PropertyValidator])
  
  def validate(settings : HashMap[String, String]) = {
    if (validateRetentionMap(settings.get(ConfConstants.schedulerVariableRetentionMap), ConfConstants.schedulerVariableRetentionMap)
        && validateRetentionMap(settings.get(ConfConstants.acumecorelevelmap), ConfConstants.acumecorelevelmap)
        && validateRetentionMap(settings.get(ConfConstants.acumecoretimeserieslevelmap), ConfConstants.acumecoretimeserieslevelmap)
        && isNumber(settings.get(ConfConstants.rrcacheconcurrenylevel), ConfConstants.rrcacheconcurrenylevel)
	    && isNumber(settings.get(ConfConstants.rrsize._1), ConfConstants.rrsize._1)
	    && isNumber(settings.get(ConfConstants.prefetchTaskRetryIntervalInMillis), ConfConstants.prefetchTaskRetryIntervalInMillis)
	    && isNumber(settings.get(ConfConstants.threadPoolSize), ConfConstants.threadPoolSize)
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
      throw new RuntimeException("Invalid acume properties...")
    }
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

    val inMemoryPolicyMap = Utility.getLevelPointMap(inMemoryPolicy)
    val diskPolicyMap = Utility.getLevelPointMap(diskPolicy)
    
    // Check whether disPolicyMap is > than inMemoryPolicyMap
    for((inMemoryLevel,inMemoryPoints) <- inMemoryPolicyMap) {
      val diskPolicyPoints = diskPolicyMap.get(inMemoryLevel).getOrElse({
        logger.error("DiskPolicyMap doesnt have all the levels configured in cachelevelPolicyMap")
        return false
      })

      if(diskPolicyPoints < inMemoryPoints) {
        logger.error("DiskPolicyMap cannot be less than inMemorylevelPolicyMap")
        return false
      }
      
//      if((inMemoryLevel.level*inMemoryPoints) < inMemoryLevel.aggregationLevel) {
//        logger.error("Combining interval is redundant.")
//        return false
//      }

//      val fraction = inMemoryLevel.aggregationLevel/inMemoryLevel.level
//      if(Math.ceil(fraction).toLong != fraction) {
//        logger.error("Combining level is not a multiple of base level")
//        return false
//      }
    }
    true
  }

}