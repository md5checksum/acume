package com.guavus.acume.cache.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.AcumeCacheConf

/**
 * @author archit.thakur
 *
 */
  
class AcumeCacheType(val name: String, val acumeCache: Class[_ <: AcumeCache[_ <: Any, _ <: Any]]) {
    def getCache(keyMap: Map[String, Any], acumeCacheContext : AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy) = {
      this match {
        case AcumeCacheType.acumeStarSchemaTreeCache => {
          new AcumeStarSchemaTreeCache(keyMap.toMap, acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy)
        }
        case AcumeCacheType.acumeFlatSchemaTreeCache => {
          new AcumeFlatSchemaTreeCache(keyMap.toMap, acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy)
        }
        case _ => acumeCache.getConstructors()(0).newInstance(keyMap.toMap, acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy)
      }
    }
  }

object AcumeCacheType extends Enumeration {

  val acumeStarSchemaTreeCache = new AcumeCacheType("AcumeStarSchemaTreeCache", classOf[AcumeTreeCache])
  val acumeFlatSchemaTreeCache = new AcumeCacheType("AcumeFlatSchemaTreeCache", classOf[AcumeFlatSchemaTreeCache])
  val cacheTypeValues = List(acumeStarSchemaTreeCache, acumeFlatSchemaTreeCache)
  
  def getAcumeCacheType(name: String) : AcumeCacheType = { 
    for(actualName <- cacheTypeValues){
      if(name equalsIgnoreCase actualName.name)
        return actualName
    }
    val cacheTypeConfigClassName = new AcumeCacheConf().get(ConfConstants.cacheTypeConfigClassName)
    Class.forName(cacheTypeConfigClassName).getConstructors()(0).newInstance(name , Class.forName(name).asInstanceOf[Class[_ <: AcumeCache[_ <: Any, _ <: Any]]]).asInstanceOf[AcumeCacheType]
  }
  
  implicit def convertValue(v: Value): AcumeCacheType = v.asInstanceOf[AcumeCacheType]
}

