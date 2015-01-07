package com.guavus.acume.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

class DummyContext extends AcumeContextTrait {

  val conf = null

  val acumeConfiguration = null  
    
  val sparkContext = null

  val hc = null

  val _sqlContext = null
  
  override val acumeContext = {
    null
  }
  
  override def sc() = sparkContext
  
  override def ac() = acumeContext
  
  override def acumeConf() = acumeConfiguration
  
  override def hqlContext() = hc
  
  override def sqlContext() = _sqlContext
  
}