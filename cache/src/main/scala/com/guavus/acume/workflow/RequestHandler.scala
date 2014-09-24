package com.guavus.acume.workflow

import com.guavus.acume.gen.Acume.Cubes.Cube
import com.guavus.acume.utility.SQLTableGetter
import com.guavus.acume.utility.SQLParserFactory
import java.io.StringReader
import net.sf.jsqlparser.statement.select.Select
import scala.collection.JavaConversions._

object RequestHandler {
  
  def handleRequest(sql: String) { 
    
    val parsedSQL = parseSql(sql)
    val tableList = parsedSQL._1
    val (startTime, endTime) = parsedSQL._2
    val cubeTableMap = tableList.map(string => (string, string.substring(0, string.indexOf("_")+1))).toMap
//    val acumeCache = CachePool.getCache
  }
  
  private [workflow] def parseSql(sql: String) = { 
    
    val sqlTableGetter = new SQLTableGetter
    val pm = SQLParserFactory.getParserManager();
    val statement = pm.parse(new StringReader(sql));
    val list = sqlTableGetter.getTableList(statement.asInstanceOf[Select]).toList.asInstanceOf[List[String]]
    val startTime = 0l // getStartTime
    val endTime = 0l // getEndTime
    (list, (startTime, endTime))
  }
} 
