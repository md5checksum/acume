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
}
