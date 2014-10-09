package com.guavus.acume.servlet

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import javax.servlet.ServletConfig
import javax.servlet.ServletException
import javax.servlet.http.HttpServlet

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.rubix.configuration.RubixProperties
import com.guavus.rubix.core.distribution.RubixDistribution
import com.guavus.rubix.hibernate.SessionContext
import com.guavus.rubix.hibernate.SessionFactory
import com.guavus.rubix.query.remote.flex.RubixService
import com.guavus.rubix.scheduler.QueryRequestPrefetchTaskManager

import RubixInitServlet._

import com.guavus.rubix.user.management.InitDatabase

import scala.collection.mutable.ArrayBuffer

import com.guavus.rubix.user.management.IDML

import org.hibernate.internal.util.ConfigHelper

object RubixInitServlet {

  private var logger: Logger = LoggerFactory.getLogger(classOf[RubixInitServlet])
}

@SerialVersionUID(2452703157821877157L)
class RubixInitServlet extends HttpServlet {

  override def init(servletConfig: ServletConfig) {
    SessionFactory.getInstance(SessionContext.DISTRIBUTED)
    logger.info("Called RubixInitServlet")
    val startTime = System.currentTimeMillis()
    val timeTaken = (System.currentTimeMillis() - startTime)
    logger.info("Time taken to fetch data from remote node {} seconds", timeTaken / 1000)
    InitDatabase.initializeDatabaseTables(ArrayBuffer[IDML]())
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.servlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.distribution.RubixDistribution;
import com.guavus.rubix.hibernate.SessionContext;
import com.guavus.rubix.hibernate.SessionFactory;
import com.guavus.rubix.query.remote.flex.RubixService;
import com.guavus.rubix.scheduler.QueryRequestPrefetchTaskManager;

|**
 * Servlet to initialize rubix at the time of server startup.
 * 
 * @author bhupesh.goel
 * 
 *|
public class RubixInitServlet extends HttpServlet {

	private static Logger logger = LoggerFactory.getLogger(RubixInitServlet.class);
	
	|**
	 * 
	 *|
	private static final long serialVersionUID = 2452703157821877157L;

	@Override
	public void init(ServletConfig servletConfig) throws ServletException {
		SessionFactory.getInstance(SessionContext.DISTRIBUTED);
		logger.info("Called RubixInitServlet");
		
		long startTime = System.currentTimeMillis();
		try{
			RubixDistribution.getInstance().fetchCacheData();
		}catch(Exception e){
			logger.warn("Error while fetching data from remote node {}",e);
		}
		long timeTaken = (System.currentTimeMillis()-startTime);
		logger.info("Time taken to fetch data from remote node {} seconds",timeTaken/1000);
		
		RubixDistribution.getInstance().waitForRehashToComplete();
		
		try{
			RubixService.getInstance().populateRRCache();
		}catch(Exception e){
			logger.warn("Error while populating rr cache {}",e);
		}
		
		if(RubixProperties.CachePreloadEnabled.getBooleanValue() && RubixProperties.CachePersistToDisk.getBooleanValue()) {
			RubixDistribution.getInstance().getBinClassToBinSourceMap();
			logger.info("PreLoading Rubix Caches");
			RubixDistribution.getInstance().cachePreload();
		} 
	}
}
*/
}