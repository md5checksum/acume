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
import com.guavus.rubix.user.management.UMProperties

/**
 * Entry point to start the tomcat. this must be called by spark or command line to start the application
 */
object AcumeMain {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeMain])

  def startAcume(args: String) {
	AcumeContextTrait.init(args, "acume")
	AcumeContextTrait.acumeContext.get.acumeConf.setSqlQueryEngine("acume")
	AcumeContextTrait.acumeContext.get.registerUserDefinedFunctions
	var enableJDBC = AcumeContextTrait.acumeContext.get.acumeConf.getEnableJDBCServer
	 
	if(Try(enableJDBC.toBoolean).getOrElse(false))
		AcumeThriftServer.main(Array[String]())
	
	//Initiate the session Factory for user management db
    SessionFactory.getInstance(SessionContext.DISTRIBUTED)
    InitDatabase.initializeDatabaseTables(ArrayBuffer[IDML]())
    UMProperties.setGlobalTimeZone(AcumeContextTrait.acumeContext.get.acumeConf.getAcumeTimeZone)
    println("Called AcumeMain")
    val startTime = System.currentTimeMillis()
    //Initialize all components for Acume Core
//    val config = ConfigFactory.getInstance()
    val timeTaken = (System.currentTimeMillis() - startTime)
    logger.info("Time taken to initialize Acume {} seconds", timeTaken / 1000)
  }
  
  
  def startHive(args: String) {
	AcumeContextTrait.init(args, "hive")
	AcumeContextTrait.acumeContext.get.acumeConf.setSqlQueryEngine("hive")
	AcumeContextTrait.acumeContext.get.registerUserDefinedFunctions
	var enableJDBC = AcumeContextTrait.acumeContext.get.acumeConf.getEnableJDBCServer
	 
	if(Try(enableJDBC.toBoolean).getOrElse(false))
		AcumeThriftServer.main(Array[String]())
	
	//Initiate the session Factory for user management db
    SessionFactory.getInstance(SessionContext.DISTRIBUTED)
    InitDatabase.initializeDatabaseTables(ArrayBuffer[IDML]())
    UMProperties.setGlobalTimeZone(AcumeContextTrait.acumeContext.get.acumeConf.getAcumeTimeZone)
    println("Called AcumeHiveMain")
    val startTime = System.currentTimeMillis()
    //Initialize all components for Acume Core

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