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

/**
 * Entry point to start the tomcat. this must be called by spark or command line to start the application
 */
object AcumeMain {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeMain])

  def startAcume(args: Array[String]) {
	  //Initiate the session Factory for user management db
    SessionFactory.getInstance(SessionContext.DISTRIBUTED)
    InitDatabase.initializeDatabaseTables(ArrayBuffer[IDML]())
    logger.info("Called AcumeMain")
    val startTime = System.currentTimeMillis()
    //Initialize all components for Acume Core
//    val config = ConfigFactory.getInstance()
    val timeTaken = (System.currentTimeMillis() - startTime)
    logger.info("Time taken to initialize Acume {} seconds", timeTaken / 1000)
  }
  
  
  /**
   * Start tomcat
   */
  def main(args: Array[String]) {
	  AcumeContext.init("acume.conf")
	  TomcatMain.startTomcatAndWait
  }
  
}

case class AcumeMain