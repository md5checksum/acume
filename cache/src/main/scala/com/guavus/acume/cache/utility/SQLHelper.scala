package com.guavus.acume.cache.utility

import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.Statement
import java.io.StringReader
import net.sf.jsqlparser.statement.select.Select

object SQLHelper {

  def getTables(sqlQuery: String) = { 
    
    val QuerySelectClauseVisitor = new QuerySelectClauseVisitor();
    val pm: CCJSqlParserManager = SQLParserFactory.getParserManager();
    val statement: Statement = pm.parse(new StringReader(sqlQuery));
    val list = Nil//sqlTableGetter.getTableList(statement.asInstanceOf[Select])
    list
  }
}