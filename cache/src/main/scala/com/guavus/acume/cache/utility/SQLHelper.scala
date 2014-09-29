package com.guavus.acume.cache.utility

import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.Statement
import java.io.StringReader

import com.guavus.acume.cache.utility.SQLParserFactory;
import com.guavus.acume.cache.utility.SQLTableGetter;

import net.sf.jsqlparser.statement.select.Select

object SQLHelper {

  def getTables(sqlQuery: String) = { 
    
    val sqlTableGetter = new SQLTableGetter();
    val pm: CCJSqlParserManager = SQLParserFactory.getParserManager();
    val statement: Statement = pm.parse(new StringReader(sqlQuery));
    val list = sqlTableGetter.getTableList(statement.asInstanceOf[Select])
    list
  }
}