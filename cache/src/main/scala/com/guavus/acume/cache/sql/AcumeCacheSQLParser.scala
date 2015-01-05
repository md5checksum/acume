package com.guavus.acume.cache.sql

import com.guavus.acume.cache.utility.Tuple
import com.guavus.acume.cache.workflow.RequestType
import com.guavus.acume.cache.workflow.RequestType._
import com.guavus.acume.cache.utility.SQLUtility

import scala.collection.JavaConversions._

/**
 * @author archit.thakur
 *
 */

class AcumeCacheSQLParser extends ISqlParser {
  
  override def parseSQL(sql: String): (List[Tuple], RequestType) = {
    
     val util = new SQLUtility()
     val list = util.getList(sql).toList
     val requestType = util.getRequestType(sql)
     (list, RequestType.getRequestType(requestType))
  }
}