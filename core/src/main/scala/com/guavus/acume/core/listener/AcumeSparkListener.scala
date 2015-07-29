package com.guavus.acume.core.listener

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager

class AcumeSparkListener extends SparkListener{
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeSparkListener])
   
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    logger.error("Spark context has gone down at ", applicationEnd.time / 1000)
    logger.error("Killing acume. Shutting down")
    System.exit(1)
  }
}

class AcumeBlockManagerRemovedListener extends SparkListener {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeSparkListener])
  
  override def onBlockManagerRemoved(blockManagerRemoved : SparkListenerBlockManagerRemoved) {
    logger.info("Block manager {} removed from spark application at : {}", blockManagerRemoved.blockManagerId, blockManagerRemoved.time/1000)
    ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).restartPrefetchScheduler
  }
}