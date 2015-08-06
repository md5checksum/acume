package com.guavus.acume.core

import net.sf.jsqlparser.statement.select.Select
import com.guavus.acume.cache.utility.SQLParserFactory
import java.io.StringReader


/**
 * @author kashish.jain
 */

abstract class DsInterpreterPolicy {
  
  def interpretDsName(query: String) : String 
  
}

class DsInterpreterPolicyImpl extends  DsInterpreterPolicy {
  
  def interpretDsName(query: String) : String = {
    val sql = SQLParserFactory.getParserManager()
    val statement = sql.parse(new StringReader(query)).asInstanceOf[Select]
    
    val queryTableName = statement.getSelectBody
    
  }
}
  