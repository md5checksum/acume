package com.guavus.acume.tomcat.core

import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.hive.thriftserver.AcumeThriftServer
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

/**
 * Entry point to start the tomcat. this must be called by spark or command line to start the application
 */
object AcumeMain {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeMain])
  
  def startAcumeComponents(sqlQueryEngine:String) = {
    System.setProperty("queryEngine", sqlQueryEngine)
    val acumeContext = ConfigFactory.getInstance.getBean(classOf[AcumeContextTrait])
	acumeContext.acumeConf.setSqlQueryEngine(sqlQueryEngine)
	acumeContext.registerUserDefinedFunctions
	
	var enableJDBC = acumeContext.acumeConf.getEnableJDBCServer
	 
	if(enableJDBC.toBoolean)
		AcumeThriftServer.main(Array[String]())
	
	//Initiate the session Factory for user management db
    SessionFactory.getInstance(SessionContext.DISTRIBUTED)
    InitDatabase.initializeDatabaseTables(ArrayBuffer[IDML]())
    UMProperties.setGlobalTimeZone(acumeContext.acumeConf.getAcumeTimeZone)
    println("Called AcumeMain on " + sqlQueryEngine)
    
    val startTime = System.currentTimeMillis()
    //start Prefetch Scheduler
    if(acumeContext.acumeConf.getEnableScheduler)
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