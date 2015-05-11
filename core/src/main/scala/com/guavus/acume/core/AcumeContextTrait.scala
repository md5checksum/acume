package com.guavus.acume.core

import scala.collection.mutable.LinkedHashMap
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

/*
 * @author kashish.jain
 */
abstract class AcumeContextTrait {
  
  def registerUserDefinedFunctions() = {
    val createStatement1 = """create temporary function PCSA_UDF as 'com.guavus.pcsa.pcsaudf.PCSAudf'"""
    hqlContext.sql(createStatement1)
    val createStatement2 = """create temporary function PCSA_UDAF as 'com.guavus.pcsa.pcsaudaf.PCSAudaf'"""
    hqlContext.sql(createStatement2)
  }
  
  val acumeContext: AcumeCacheContextTrait = null

  def sc(): SparkContext = null

  def ac(): AcumeCacheContextTrait = null

  def acumeConf(): AcumeConf = null

  def hqlContext(): HiveContext = null

  def sqlContext(): SQLContext = null

}

object AcumeContextTrait {
  val logger: Logger = LoggerFactory.getLogger(AcumeContextTrait.getClass)
  var acumeContext: Option[AcumeContextTrait] = None
  val accumulatorMap = new LinkedHashMap[String, Accumulator[Long]]

  def init(confFilePath: String, queryEngineType: String): AcumeContextTrait = acumeContext.getOrElse({
    acumeContext = if (queryEngineType.equals("acume"))
      Some(new AcumeContext(confFilePath))
    else
      Some(new AcumeHiveContext(confFilePath))
    acumeContext.get
  })

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

  def clearAccumulator() {
    for (key <- accumulatorMap.keys) {
      accumulatorMap.get(key).get.value = 0L
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
        accumulatorMap.get(key).get.value = 0L
      }
    }
  }
}
