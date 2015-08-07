package com.guavus.acume.cache.disk.utility

import com.guavus.insta.Insta
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.common.ConfConstants
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil
import com.typesafe.config.ConfigFactory


/**
 * @author kashish.jain
 */
object InstaUtil {
  
  var insta: Insta = null
  
  def initializeInstaClient(hiveContext: HiveContext) : Insta = {
    //Insta constructor requires HiveContext now, so we have to do explicit typecasting here
    insta = new Insta(hiveContext)
    insta.init(AcumeCacheContextTraitUtil.cacheConf.get(ConfConstants.backendDbName), AcumeCacheContextTraitUtil.cacheConf.get(ConfConstants.cubedefinitionxml))
    insta
  }
  
  def getInstaClient : Insta = {
    if(insta == null)
      throw new RuntimeException("Insta client is not created yet")
    else
      insta
  }
    
}