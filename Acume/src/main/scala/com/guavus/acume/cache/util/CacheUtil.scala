package com.guavus.acume.cache.util

import com.guavus.acume.cache.EvictionDetails
import org.apache.commons.configuration.PropertiesConfiguration
import scala.collection.mutable.{Map => MutableMap}

class CacheUtil {

  def parseEvictionDetailsMapFromFile(): Map[String, EvictionDetails] = {
    var properties: PropertiesConfiguration = null
    val evictionDetailsMap = MutableMap[String, EvictionDetails]()
    try {
      properties = new PropertiesConfiguration()
      properties.setDelimiterParsingDisabled(true)
      properties.load("evictiondetails.properties")
      val keySet = properties.getKeys
      while (keySet.hasNext) {
        val key = keySet.next().asInstanceOf[String]
        val value = properties.getString(key)
        if (value != null) {
          val valuesArr = value.split("\\|")
          if (valuesArr.length == 1 && !value.contains("|")) {
            try {
              val memoryEvictionCount = java.lang.Integer.parseInt(valuesArr(0))
              val evictionDetails = new EvictionDetails()
              evictionDetails.setMemoryEvictionThresholdCount(memoryEvictionCount)
              evictionDetailsMap.put(key, evictionDetails)
            } catch {
              case e: Exception => {
//                logger.error("Error " + e + 
//                  " in parseEvictionDetailsMapFromFile while parsing " + 
//                  key)
                //continue
              }
            }
          } else if (valuesArr.length > 1 || (valuesArr.length == 1 && value.contains("|"))) {
            val policyName = valuesArr(0)
            var retentionMapString = ""
            var flashRetentionMapString = ""
            var diskRetentionString = ""
            var diskRetentionCombinePointString = ""
            if (valuesArr.length > 1) {
              retentionMapString = valuesArr(1)
            }
            if (valuesArr.length > 2) {
              diskRetentionString = valuesArr(2)
            }
            if (valuesArr.length > 3) {
              diskRetentionCombinePointString = valuesArr(3)
            }
            if (valuesArr.length > 4) {
              flashRetentionMapString = valuesArr(4)
            }
            try {
              val evictionDetails = new EvictionDetails()
              if (StringUtils.isNotBlank(policyName)) {
                Class.forName(policyName)
                evictionDetails.setEvictionPolicyName(policyName)
              }
              if (StringUtils.isNotBlank(retentionMapString)) {
                val retentionMap = Utility.getLevelPointMap(retentionMapString)
                evictionDetails.setVariableRetentionMap(retentionMap)
              }
              if (StringUtils.isNotBlank(flashRetentionMapString)) {
                val flashRetentionMap = Utility.getLevelPointMap(flashRetentionMapString)
                evictionDetails.setFlashVariableRetentionMap(flashRetentionMap)
              }
              if (StringUtils.isNotBlank(diskRetentionString)) {
                val diskRetentionMap = Utility.getLevelPointMap(diskRetentionString)
                evictionDetails.setDiskRetentionMap(diskRetentionMap)
              }
              if (StringUtils.isNotBlank(diskRetentionCombinePointString)) {
                val diskRetentionCombinePointMap = Utility.getDiskPersistLevelPointMap(diskRetentionCombinePointString)
                evictionDetails.setDiskRetentionCombinePointMap(diskRetentionCombinePointMap)
              }
              evictionDetailsMap.put(key, evictionDetails)
            } catch {
              case e: Exception => {
                logger.error("Error " + e + 
                  " in parseEvictionDetailsMapFromFile while parsing " + 
                  key)
                //continue
              }
            }
          } else {
//            logger.error("Error in parseEvictionDetailsMapFromFile while parsing " + 
              key)
          }
        }
      }
    } catch {
      case e: Throwable => {
//        logger.error("Error " + e + " in parseEvictionDetailsMapFromFile...")
        e.printStackTrace()
      }
    }
    evictionDetailsMap
  }
}
