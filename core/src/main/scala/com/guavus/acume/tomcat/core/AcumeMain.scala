
package com.guavus.acume.tomcat.core

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager
import com.guavus.rubix.hibernate.SessionContext
import com.guavus.rubix.hibernate.SessionFactory
import com.guavus.rubix.user.management.IDML
import com.guavus.rubix.user.management.InitDatabase
import com.guavus.rubix.user.management.UMProperties
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.core.AcumeContextTraitUtil
import org.apache.spark.sql.hive.thriftserver.AcumeHiveThriftServer2
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2


/**
 * Entry point to start the tomcat. this must be called by spark or command line to start the application
 */
object AcumeMain {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeMain])
  
  def startAcumeComponents = {
    
    val startTime = System.currentTimeMillis

     /*
     * Initiate the session Factory for user management db
     */
    SessionFactory.getInstance(SessionContext.DISTRIBUTED)
    InitDatabase.initializeDatabaseTables(ArrayBuffer[IDML]())
    UMProperties.setGlobalTimeZone(AcumeContextTraitUtil.acumeConf.getAcumeTimeZone)
    logger.info("Called AcumeMain")
    
    /*
     * Start Prefetch Scheduler
     */
    val queryPrefetcher = ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager])
    val numberOfEnabledSchedulers =  AcumeContextTraitUtil.acumeConf.getEnabledDatasourceNames.filter(dsName => AcumeContextTraitUtil.acumeConf.getEnableScheduler(dsName)).size
    if(numberOfEnabledSchedulers != 0)
    	queryPrefetcher.startPrefetchScheduler
   

    /*
     * Start thriftServer
     */
    if(AcumeContextTraitUtil.acumeConf.getEnableJDBCServer.toBoolean) {
      if(AcumeContextTraitUtil.acumeConf.getBoolean(ConfConstants.thriftIsRawQuery).getOrElse(true))
        HiveThriftServer2.startWithContext(AcumeContextTraitUtil.hiveContext)
      else
        AcumeHiveThriftServer2.startWithContext(AcumeContextTraitUtil.hiveContext)
    }

    val timeTaken = (System.currentTimeMillis() - startTime)
    logger.info("Time taken to initialize Acume {} seconds", timeTaken / 1000)
  
  }
  
  /**
   * Start tomcat
   */
  def main(args: Array[String]) {
	  TomcatMain.startTomcatAndWait
  }
  
}

case class AcumeMain