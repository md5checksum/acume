package com.guavus.acume.core

import org.apache.spark.sql.SQLContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.workflow.AcumeCacheContext

/**
 * @author pankaj.arora
 *
 * This will keep the sparkcontext and hive context.
 */
class AcumeContext(val acumeConfiguration: AcumeConf) extends AcumeContextTrait {

  val _sqlContext = new SQLContext(sparkContext)
  val acumeContext = new AcumeCacheContext(_sqlContext, new AcumeCacheConf)
  
  override def ac() = acumeContext
  
  override def sqlContext() = _sqlContext
  
}
