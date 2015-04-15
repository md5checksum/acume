package com.guavus.acume.core

import scala.collection.mutable.LinkedHashMap
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import javax.xml.bind.JAXBContext
import java.io.FileInputStream
import com.guavus.acume.core.gen.AcumeUdfs
import scala.collection.JavaConversions._
import java.io.File
import java.io.InputStream
import java.nio.file.Files

/*
 * @author kashish.jain
 */
abstract class AcumeContextTrait {
  
  val acumeConfiguration: AcumeConf

  val acumeContext: AcumeCacheContextTrait = null

  def sc(): SparkContext = null

  def ac(): AcumeCacheContextTrait = null

  def acumeConf(): AcumeConf = AcumeConf.acumeConf

  def hqlContext(): HiveContext = null

  def sqlContext(): SQLContext = null

  def registerUserDefinedFunctions() =
    {
      val xml = this.acumeConf.getUdfConfigurationxml
      val jc = JAXBContext.newInstance("com.guavus.acume.core.gen")
      val unmarsh = jc.createUnmarshaller()

      var inputstream: InputStream = null
      if (new File(xml).exists())
        inputstream = new FileInputStream(xml)
      else {
        inputstream = this.getClass().getResourceAsStream("/" + xml)
        if (inputstream == null)
          throw new RuntimeException(s"$xml file does not exists")
      }

      val acumeUdf = unmarsh.unmarshal(inputstream).asInstanceOf[AcumeUdfs]
      var udf: AcumeUdfs.UserDefined = null
      for (udf <- acumeUdf.getUserDefined()) {
        val createStatement = "create temporary function " + udf.getFunctionName() + " as '" + udf.getFullUdfclassName() + "'"
        hqlContext.sql(createStatement)
      }
    }
}

object AcumeContextTrait {
  val logger: Logger = LoggerFactory.getLogger(AcumeContextTrait.getClass)
  var acumeContext: Option[AcumeContextTrait] = None
  val accumulatorMap = new LinkedHashMap[String, Accumulator[Long]]
  
  val acumeConf = new AcumeConf(true, this.getClass.getResourceAsStream("/acume.conf"))

  def init(queryEngineType : String):AcumeContextTrait = acumeContext.getOrElse({
    acumeContext = if(queryEngineType.equals("acume"))
    	Some(new AcumeContext(acumeConf))
    else
    	Some(new AcumeHiveContext(acumeConf))
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
