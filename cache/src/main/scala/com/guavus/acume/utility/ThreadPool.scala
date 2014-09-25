package com.guavus.acume.utility

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import org.apache.spark.Logging

object ThreadPool extends Logging { 
  
  var exservice: Option[ExecutorService] = None
  
  def prepThreadPool(numThreads: Int): Boolean = {
	exservice match {
	  case None => 
	    exservice = Some(Executors.newFixedThreadPool(numThreads, new ModifiedThreadFactory("AcumeThreadPool")))
	    true
	  case Some(ex) => 
	    logDebug("ThreadPool has already been initialized.")
	    false
	}
  }
  
  def getExService() = exservice

}
