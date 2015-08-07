package com.guavus.acume.core

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

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

}
