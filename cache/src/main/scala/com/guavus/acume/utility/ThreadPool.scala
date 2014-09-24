package com.guavus.acume.utility

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

object ThreadPool extends Log { 
  
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
