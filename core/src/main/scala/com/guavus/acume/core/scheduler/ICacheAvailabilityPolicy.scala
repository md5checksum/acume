package com.guavus.acume.core.scheduler

import scala.collection.mutable.HashMap
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.core.Interval
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.core.AcumeContextTrait
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * 
 * This CacheUpdate policy interacts with three componenets of acume cache system - 
 * 1. scheduler - for true cache availability map, reseting the map, or updating it.
 * 2. listeners - for handling the already progressed map, maybe saving it 
 * or updating it the special way in case of some event which could be removal of an executor or anything else.
 * 3. cache - preprocessing the individual rdds before making the runnable dataset 
 * or getting the cache availabilty map while making checks around timestamps of the query recieved. 
 * 
 * @author archit.thakur
 */
abstract class ICacheAvalabiltyUpdatePolicy(acumeConf: AcumeConf, sqlContext: SQLContext) {
  
  private val acumeCacheAvailabilityMap: HashMap[String, HashMap[Long, Interval]] = HashMap[String, HashMap[Long, Interval]]()
  
  /**
   * should not be overriden.
   * API targeted for scheduler.
   */
  def getTrueCacheAvailabilityMap: HashMap[String, HashMap[Long, Interval]] = {
    acumeCacheAvailabilityMap
  }
  
  /**
   * should be overriden.
   * API targeted for solutions' code
   */
  def getCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = {
    acumeCacheAvailabilityMap
  }
    
  /**
   * should be overriden.
   * API targeted for listeners.
   */
  def onBlockManagerRemoved(withMap: HashMap[String, HashMap[Long, Interval]] = HashMap[String, HashMap[Long, Interval]]()): Unit = {
    acumeCacheAvailabilityMap.clear()
    if(!withMap.isEmpty) acumeCacheAvailabilityMap.++=(withMap)
  }
  
  /**
   * 
   * should be overriden.
   * API targeted for listeners.
   */
  def onBackwardCombinerCompleted(version: Int) {
    //do nothing here
  }
  
  /**
   * should be overriden.
   * API targeted for cache used. 
   */
  def preProcessSchemaRDD(unprocessed: SchemaRDD): SchemaRDD = {
    
    val processed = unprocessed
    processed
  }
  
  /**
   * can be overriden.
   * API could be used by any component.
   */
  def update(withMap: HashMap[String, HashMap[Long, Interval]] = HashMap[String, HashMap[Long, Interval]]()): Unit = {
//    acumeCacheAvailabilityMap.clear()
    if(!withMap.isEmpty) acumeCacheAvailabilityMap.++=(withMap)
  }
  
  /**
   * should not be overriden.
   * API could be used by any component.
   */
  def reset: Unit = {
    acumeCacheAvailabilityMap.clear()
  }
}

object ICacheAvalabiltyUpdatePolicy {
  
  val objectgetter = HashMap[String, ICacheAvalabiltyUpdatePolicy]()
  def getICacheAvalabiltyUpdatePolicy(acumeConf: AcumeConf, sqlContext: SQLContext): ICacheAvalabiltyUpdatePolicy = {
    val _$key = ConfConstants.acumecacheavailablitymappolicy
    val _$value = objectgetter.getOrElse(_$key, Class.forName(acumeConf.getOption(_$key).getOrElse("com.guavus.acume.core.scheduler.AcumeCacheAvailabiltyPolicy")).getConstructor(classOf[AcumeConf], classOf[SQLContext]).newInstance(acumeConf, sqlContext)
    .asInstanceOf[ICacheAvalabiltyUpdatePolicy])
    if(!objectgetter.contains(_$key)) {
      objectgetter.put(_$key, _$value)
    }
    _$value
  }
}