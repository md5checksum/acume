package com.guavus.acume.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.workflow.AcumeHiveCacheContext
import com.guavus.acume.core.listener.AcumeSparkListener
import com.guavus.acume.cache.common.ConfConstants

/**
 * @author kashish.jain
 * 
 */
class AcumeHiveContext(val acumeConfiguration: AcumeConf) extends AcumeContextTrait {

  //Properties will be loaded from spark-defaults.conf
  val conf = new SparkConf()
  //conf.set("spark.app.name", "Acume")
    
  val sparkContext = new SparkContext(conf)
  sparkContext.addSparkListener(new AcumeSparkListener )

  val hc = new HiveContext(sparkContext)

  val _sqlContext = new SQLContext(sparkContext)
  
  override val acumeContext = {
    new AcumeHiveCacheContext(hc, new AcumeCacheConf)
  }
  
  def chooseHiveDatabase() {
    try{
       hc.sql("use " + acumeConfiguration.get(ConfConstants.backendDbName))
    } catch {
      case ex : Exception => throw new RuntimeException("Cannot use the database " + acumeConfiguration.get(ConfConstants.backendDbName), ex)
    }
  }
  
  chooseHiveDatabase()

  override def sc() = sparkContext
  
  override def ac() = acumeContext
  
  override def acumeConf() = acumeConfiguration
  
  override def hqlContext() = hc
  
  override def sqlContext() = _sqlContext
  
}
