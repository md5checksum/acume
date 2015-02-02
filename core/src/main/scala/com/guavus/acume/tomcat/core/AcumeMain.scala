package com.guavus.acume.tomcat.core

import com.guavus.rubix.user.management.InitDatabase
import com.guavus.rubix.hibernate.SessionFactory
import com.guavus.acume.core.configuration.ConfigFactory
import scala.collection.mutable.ArrayBuffer
import com.guavus.rubix.hibernate.SessionContext
import com.guavus.rubix.user.management.IDML
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.core.AcumeContext
import org.apache.spark.sql.hive.thriftserver.AcumeThriftServer
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.configuration.AcumeAppConfig
import com.guavus.acume.cache.common.ConfConstants
import scala.util.Try
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.scheduler.QueryRequestPrefetchTaskManager

/**
 * Entry point to start the tomcat. this must be called by spark or command line to start the application
 */
object AcumeMain {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeMain])

  def startAcume(args: String) {
	AcumeContextTrait.acumeContext.get.acumeConf.setSqlQueryEngine("acume")
	initializeComponents(args)
  }
  
  def startHive(args: String) {
    AcumeContextTrait.acumeContext.get.acumeConf.setSqlQueryEngine("hive")
	initializeComponents(args)
  }
  
  private def initializeComponents(args: String) = {
    
    val queryEngine = AcumeContextTrait.acumeContext.get.acumeConf.getSqlQueryEngine
    
     try {
      AcumeContextTrait.init(args, queryEngine)
    } catch{
      case ex : Throwable => {
        println("Initialization failed. Exiting...")
        System.exit(1)
      }
    }
    
    var enableJDBC = AcumeContextTrait.acumeContext.get.acumeConf.getEnableJDBCServer
	 
	if(Try(enableJDBC.toBoolean).getOrElse(false))
		AcumeThriftServer.main(Array[String]())
	
	//Initiate the session Factory for user management db
    SessionFactory.getInstance(SessionContext.DISTRIBUTED)
    InitDatabase.initializeDatabaseTables(ArrayBuffer[IDML]())
    println("Called AcumeMain on " + queryEngine)
    
    val startTime = System.currentTimeMillis()
    
    //start Prefetch Scheduler
    if(AcumeContextTrait.acumeContext.get.acumeConf.getEnableScheduler)
    	ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).startPrefetchScheduler

    //Initialize all components for Acume Core
    //val config = ConfigFactory.getInstance()
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