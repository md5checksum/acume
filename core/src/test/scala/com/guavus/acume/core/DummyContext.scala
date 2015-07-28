package com.guavus.acume.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

class DummyContext extends AcumeContextTrait {

  val acumeConfiguration = null  
    
  val _sqlContext = null
  
  val acumeContext = null
  
  override def acc() = acumeContext
  
  override def sqlContext() = _sqlContext
  
}