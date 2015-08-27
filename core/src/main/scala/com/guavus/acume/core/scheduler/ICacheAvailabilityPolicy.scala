package com.guavus.acume.core.scheduler

import scala.collection.mutable.HashMap
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.core.Interval
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.core.AcumeContextTrait
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.guavus.acume.core.configuration.ConfigFactory

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
abstract class ICacheAvalabilityUpdatePolicy(acumeConf: AcumeConf, sqlContext: SQLContext) extends Serializable {
  
  protected var mode = "full"
  private var acumeCacheAvailabilityMapWithVersion: HashMap[Int, HashMap[String, HashMap[Long, Interval]]] = HashMap[Int, HashMap[String, HashMap[Long, Interval]]]()
  
  /**
   * should not be overriden.
   * API targeted for scheduler.
   */
  def getTrueCacheAvailabilityMap(version: Int): HashMap[String, HashMap[Long, Interval]] = {
    if(acumeCacheAvailabilityMapWithVersion.contains(version))
      return acumeCacheAvailabilityMapWithVersion.get(version).get
    else {
      this.synchronized {
        return acumeCacheAvailabilityMapWithVersion.getOrElseUpdate(version, HashMap[String, HashMap[Long, Interval]]())
      }
    }
  }
  
  protected def getLocalTrueCacheAvailabilityMap(version: Int): HashMap[String, HashMap[Long, Interval]] = {

    if(acumeCacheAvailabilityMapWithVersion.contains(version))
      return acumeCacheAvailabilityMapWithVersion.get(version).get.clone
    else {
      return HashMap[String, HashMap[Long, Interval]]()
    }
  }
  
  /**
   * should be overriden.
   * API targeted for solutions' code
   */
  def getCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = {
    val _$version = ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).getVersion
    this.getLocalTrueCacheAvailabilityMap(_$version)
  }
    
  /**
   * should be overridden.
   * API targeted for listeners.
   */
  def onBlockManagerRemoved: Unit = {
//    val _$version = ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).getVersion
//    acumeCacheAvailabilityMapWithVersion.+=(_$version -> HashMap[String, HashMap[Long, Interval]]())
  }
  
  /**
   * 
   * should be overridden.
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
   * should not be overriden.
   * API could be used by any component.
   */
  private[core] def reset(version: Int): Unit = {
    this.synchronized {
      acumeCacheAvailabilityMapWithVersion.+=(version -> HashMap[String, HashMap[Long, Interval]]())
    }
  }
  
  /**
   * should not be overriden.
   */
  def getMode = mode
  
  /**
   * should not be overriden.
   */
  def setMode(argmode: String) {
    this.mode = argmode
  }
}

object ICacheAvalabiltyUpdatePolicy {
  
  val objectgetter = HashMap[String, ICacheAvalabilityUpdatePolicy]()
  def getICacheAvalabiltyUpdatePolicy(acumeConf: AcumeConf, sqlContext: SQLContext): ICacheAvalabilityUpdatePolicy = {
    val _$key = ConfConstants.acumecacheavailablitymappolicy
    if (objectgetter.contains(_$key))
      return objectgetter.get(_$key).get.asInstanceOf[ICacheAvalabilityUpdatePolicy]

    this.synchronized {
      val _$value = objectgetter.getOrElse(_$key, Class.forName(acumeConf.getOption(_$key).getOrElse("com.guavus.acume.core.scheduler.AcumeCacheAvailabilityPolicy")).getConstructor(classOf[AcumeConf], classOf[SQLContext]).newInstance(acumeConf, sqlContext)
        .asInstanceOf[ICacheAvalabilityUpdatePolicy])
      if (!objectgetter.contains(_$key)) {
        objectgetter.put(_$key, _$value)
      }
      _$value
    }
    
  }
}
