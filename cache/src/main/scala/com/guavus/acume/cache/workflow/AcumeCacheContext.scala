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
  
  private def checkQLValidation(sqlContext: SQLContext, qltype: QLType) = { 
    
    sqlContext match{
      case hiveContext: HiveContext =>
        qltype match{
          case QLType.hql | QLType.sql => true
          case rest => false
        }
      case sqlContext: SQLContext => 
        qltype match{
          case QLType.sql => true
          case rest => false
        }
      case rest => throw new RuntimeException("this type of $sqlContext is not supported")
    }
  }
  
  private def getQLType() = QLType.getQLType(conf.get(ConfConstants.qltype)) 	
  
  def acql(sql: String, qltype: String) = { 
    
    val ql = QLType.getQLType(qltype)
    if(!checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"$ql not supported with $sqlContext")
    
    val parsedSQL = parseSql(sql)
    val tableList = parsedSQL._1
    val (startTime, endTime) = parsedSQL._2
    val tblCbeMap = tableList.map(string => (string, string.substring(0, string.indexOf("_")+1))).toMap
//    val systemloader = AcumeCacheFactory.getAcumeCache(name, conf.get(ConfConstants.whichcachetouse))
  }
  
  def acql(sql: String) = { 
    
    val ql = getQLType()
    if(!checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"$ql not supported with $sqlContext")
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
  
  private[cache] def ACQL(qltype: QLType, sqlContext: SQLContext) = { 
    
    if(sqlContext.isInstanceOf[HiveContext]){
    
      qltype match{
      case QLType.hql => sqlContext.asInstanceOf[HiveContext].hql(_)
      case QLType.sql => sqlContext.sql(_)
      }
    }
    else if(sqlContext.isInstanceOf[SQLContext]) { 
      
      qltype match{
      case QLType.sql => sqlContext.sql(_)
      }
    }
  }
}

