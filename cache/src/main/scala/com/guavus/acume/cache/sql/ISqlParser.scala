package com.guavus.acume.cache.sql

import com.guavus.acume.cache.utility.Tuple
import com.guavus.acume.cache.workflow.RequestType._
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import scala.collection.mutable.{HashMap => SHashMapMutable}


/**
 * @author archit.thakur
 *
 */

trait ISqlParser {
  
  def parseSQL(sql: String): (List[Tuple], RequestType)
}

object ISqlParser {
  
  val hashmap = new SHashMapMutable[String, ISqlParser]
  def getSqlParser(conf: AcumeCacheConf): ISqlParser = {
    
    val key = conf.get(ConfConstants.acumecachesqlparser)
    hashmap.get(key) match {
      case Some(sqlparser) => sqlparser
      case None => val acumecachesqlparserclz = Class.forName(key)
      val acumecachesqlparser = acumecachesqlparserclz.newInstance().asInstanceOf[ISqlParser]
      hashmap.put(key, acumecachesqlparser)
      acumecachesqlparser
    }
  }
}


