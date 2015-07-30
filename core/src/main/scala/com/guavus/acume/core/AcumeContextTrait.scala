package com.guavus.acume.core

import java.io.File
import java.io.FileInputStream
import java.io.InputStream

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.LinkedHashMap

import org.apache.spark.Accumulator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.core.gen.AcumeUdfs
import com.guavus.acume.core.listener.AcumeSparkListener

import javax.xml.bind.JAXBContext

/*
 * @author kashish.jain
 */
abstract class AcumeContextTrait {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeContextTrait])
  
  /*
   *  TO be overrided by derived classes
   */
  def acc(): AcumeCacheContextTrait

  def sqlContext(): SQLContext
  
  def sc(): SparkContext = AcumeContextTraitUtil.sparkContext
    
  def acumeConf(): AcumeConf = AcumeConf.acumeConf

  lazy val cacheBaseDirectory : String = getCacheBaseDirectory

  def init() {
    //initialize anything
    // This must be called after creating acumeContext
    cacheBaseDirectory
  }
  
  protected def getCacheBaseDirectory() = {
	  val diskBaseDirectory = Utility.getDiskBaseDirectory(acc)
			  
	  val checkpointDirectory = diskBaseDirectory + File.separator + "checkpoint"
	  Utility.deleteDirectory(checkpointDirectory, acc)
	  sc.setCheckpointDir(checkpointDirectory)
	  println(s"setting checkpoint directory as $checkpointDirectory")
	  diskBaseDirectory
  } 
 
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
        sqlContext.sql(createStatement)
      }
    }
}
