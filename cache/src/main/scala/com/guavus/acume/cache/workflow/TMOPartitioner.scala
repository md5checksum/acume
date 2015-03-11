package com.guavus.acume.cache.workflow

import org.apache.spark.HashPartitioner

object TMOPartitioner {

  //TODO: Configure
  lazy val partitioner = new HashPartitioner(System.getProperty("acume.cache.partitions").toInt)
  
  def getPartition(key: Any) = {
    partitioner.getPartition(key)
  }
}