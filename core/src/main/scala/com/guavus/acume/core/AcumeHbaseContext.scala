package com.guavus.acume.core

import org.apache.spark.sql.hbase.HBaseSQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeHbaseCacheContext

class AcumeHbaseContext(val datasoureName: String) extends AcumeContextTrait {
  
  private val hbaseContext = AcumeContextTraitUtil.hBaseSQLContext
  private val acumeCacheContext = new AcumeHbaseCacheContext(hbaseContext, new AcumeCacheConf(datasoureName))

  override def acc() = acumeCacheContext

  override def sqlContext() = hbaseContext

}