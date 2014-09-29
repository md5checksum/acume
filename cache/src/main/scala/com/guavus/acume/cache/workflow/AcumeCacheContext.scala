package com.guavus.acume.cache.workflow

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import java.lang.UnsupportedOperationException
import com.guavus.acume.common.QLType
import com.guavus.acume.common.QLType._
import com.guavus.acume.cache.utility.SQLParserFactory
import com.guavus.acume.cache.utility.SQLTableGetter
import com.guavus.acume.cache.core.AcumeCacheFactory
import java.io.StringReader
import net.sf.jsqlparser.statement.select.Select
import scala.collection.JavaConversions._

class AcumeCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) { 
  
  def acql(sql: String, qltype: QLType) = { 
    
    val parsedSQL = parseSql(sql)
    val tableList = parsedSQL._1
    val (startTime, endTime) = parsedSQL._2
    val tblCbeMap = tableList.map(string => (string, string.substring(0, string.indexOf("_")+1))).toMap
//    val systemloader = AcumeCacheFactory.getAcumeCache(name, conf.get(ConfConstants.whichcachetouse))
  }
  
  def acql(sql: String) = { 
    
    val parsedSQL = parseSql(sql)
    val tableList = parsedSQL._1
    val (startTime, endTime) = parsedSQL._2
    val tblCbeMap = tableList.map(string => (string, string.substring(0, string.indexOf("_")+1))).toMap
//    val systemloader = AcumeCacheFactory.getAcumeCache(name, conf.get(ConfConstants.whichcachetouse))
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
  
  private[cache] def ACQL(ishql: Boolean) = { 
    
    if(sqlContext.isInstanceOf[HiveContext]){
    
      ishql match{
      case true => sqlContext.asInstanceOf[HiveContext].hql(_)
      case false => sqlContext.sql(_)
      }
    }
    else if(sqlContext.isInstanceOf[SQLContext]) { 
      
      ishql match{
      case true => throw new UnsupportedOperationException("hql is not supported on SQLContext object.")
      case false => sqlContext.sql(_)
      }
    }
    else { 
      
      throw new UnsupportedOperationException("object has to be HiveContext or SQLContext object.")
    }
  }
  
  private[cache] def ACQL = { 
    
    if(sqlContext.isInstanceOf[HiveContext]){
    
      conf.get(ConfConstants.ishql) match{
      case "true" => sqlContext.asInstanceOf[HiveContext].hql(_)
      case "false" => sqlContext.sql(_)
      }
    }
    else if(sqlContext.isInstanceOf[SQLContext]) { 
      
      conf.get(ConfConstants.ishql) match{
      case "true" => throw new UnsupportedOperationException("hql is not supported on SQLContext object.")
      case "false" => sqlContext.sql(_)
      }
    }
    else { 
      
      throw new UnsupportedOperationException("object has to be HiveContext or SQLContext object.")
    }
  }
}

