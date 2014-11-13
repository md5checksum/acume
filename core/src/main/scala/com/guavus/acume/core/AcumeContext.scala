package com.guavus.acume.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import org.apache.spark.Accumulator
import scala.collection.mutable.HashMap
import scala.collection.mutable.LinkedHashMap
import org.slf4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf

/**
 * @author pankaj.arora
 *
 * This will keep the sparkcontext and hive context.
 */
class AcumeContext(confFilePath: String) {

  //Properties will be loaded from spark-defaults.conf
  val conf = new SparkConf()
  conf.set("spark.app.name", "Acume")

  val acumeConfiguration = new AcumeConf(true, this.getClass.getResourceAsStream(confFilePath))  
    
  val sparkContext = new SparkContext(conf)

  val hc = new HiveContext(sparkContext)

  val _sqlContext = new SQLContext(sparkContext)
  
  val acumeContext = new AcumeCacheContext(hc, new AcumeCacheConf)
  
  def sc() = sparkContext
  
  def ac() = acumeContext
  
  def acumeConf() = acumeConfiguration
  
  def hqlContext() = hc
  
  def sqlContext() = _sqlContext
  
}

object AcumeContext {
  val logger: Logger = LoggerFactory.getLogger(AcumeContext.getClass)
  var acumeContext: Option[AcumeContext] = None
  val accumulatorMap = new LinkedHashMap[String, Accumulator[Long]]
  def init(confFilePath : String) = acumeContext.getOrElse(acumeContext = Some(new AcumeContext(confFilePath)))

  def stop() {
    logger.info("Destroying Acume Context")
    acumeContext.getOrElse(throw new IllegalArgumentException("Destroying context without initializing it.")).sc.stop
    acumeContext = None
  }

  def getAccumulator(name: String): Option[Accumulator[Long]] = {
    accumulatorMap.get(name)
  }

  def addAccumulator(name: String, accumulator: Option[Accumulator[Long]]) {
    if (!accumulator.isEmpty) {
      accumulatorMap += (name -> accumulator.get)
    }
  }

  def clearAccumulator(){
    for (key <- accumulatorMap.keys) {
    	accumulatorMap.get(key).get.value=0L       
      }
  }
  
  def printAndClearAccumulator() {
    val sparkConf = acumeContext.get.sc().getConf
    val executorCores = if (sparkConf.getOption("spark.executor.cores").isEmpty) 1; else sparkConf.get("spark.executor.cores").toInt
    val executorInstances = if (sparkConf.getOption("spark.executor.instances").isEmpty) 2; else sparkConf.get("spark.executor.instances").toInt
    val totalCores = executorCores * executorInstances
    for (key <- accumulatorMap.keys) {
      val value = accumulatorMap(key).value
      if (value > 0) {
        accumulatorMap.get(key);
        logger.debug("Accumulator " + key + " =  " + value / (totalCores))
        accumulatorMap.get(key).get.value=0L
      }
    }
  }
}