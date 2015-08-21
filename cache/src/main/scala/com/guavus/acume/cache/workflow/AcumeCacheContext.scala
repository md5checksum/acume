package com.guavus.acume.cache.workflow

import java.io.StringReader
import java.util.Random

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.core.AcumeCacheFactory
import com.guavus.acume.cache.core.CacheIdentifier
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.sql.ISqlCorrector
import com.guavus.acume.cache.utility.SQLParserFactory
import com.guavus.acume.cache.utility.SQLUtility
import com.guavus.acume.cache.utility.Tuple

import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.Parenthesis
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select

/**
 * @author archit.thakur
 *
 */
class AcumeCacheContext(cacheSqlContext: SQLContext, cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait(cacheSqlContext, cacheConf) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeCacheContext])
  
  override val dataLoader = DataLoader.getDataLoader(this, cacheConf, null)
  
  override private [acume] def executeQuery(sql: String) = {
    
    val originalparsedsql = AcumeCacheContext.parseSql(sql)
    
    logger.info("AcumeRequest obtained " + sql)
    var correctsql = ISqlCorrector.getSQLCorrector(cacheConf).correctSQL(this, sql, (originalparsedsql._1.toList, originalparsedsql._2))
    var updatedsql = correctsql._1._1
    val queryOptionalParams = correctsql._1._2
    var updatedparsedsql = correctsql._2
    
    val rt = updatedparsedsql._2
      
    var i = ""
    val list = for(l <- updatedparsedsql._1) yield {
      val cube = l.getCubeName
      val binsource = l.getBinsource
      val startTime = l.getStartTime
      val endTime = l.getEndTime
    
      validateQuery(startTime, endTime, binsource)
      
      val key_binsource = 
        if(binsource != null)
          binsource
      else
        cacheConf.get(ConfConstants.acumecorebinsource)

      i = AcumeCacheContext.getTable(cube)
      val id = getCube(CubeKey(cube, key_binsource))
      updatedsql = updatedsql.replaceAll(s"$cube", s"$i")
      val idd = new CacheIdentifier()
      idd.put("cube", id.hashCode)
      val instance = AcumeCacheFactory.getInstance(this, cacheConf, idd, id)
      if(l.getSingleEntityKeyValueList() == null  || l.getSingleEntityKeyValueList().size == 0) {
    	  instance.createTempTableAndMetadata(List(Map[String, Any]()), startTime, endTime, rt, i,Some(queryOptionalParams))
      } else {
        val singleEntityKeys = (for(singleEntityKeys <- l.getSingleEntityKeyValueList()) yield {
          singleEntityKeys.map(x => (x._1 -> x._2.asInstanceOf[Any])).toMap
        }).toList
        instance.createTempTableAndMetadata(singleEntityKeys, startTime, endTime, rt, i,Some(queryOptionalParams))
      }
    }
    
    val klist = list.flatMap(_.timestamps)
    val kfg = execute(updatedsql)
    AcumeCacheResponse(kfg, kfg.rdd, MetaData(-1, klist))
  }
  
  private [acume] def validateQuery(startTime : Long, endTime : Long, binSource : String) {
    if(startTime < BinAvailabilityPoller.getFirstBinPersistedTime(binSource) || endTime > BinAvailabilityPoller.getLastBinPersistedTime(binSource)){
      throw new RuntimeException("Cannot serve query. StartTime and endTime doesn't fall in the availability range.")
    }
  }
  
  private [acume] def execute(updatedsql: String) = {
    AcumeCacheContext.ACQL(cacheSqlContext)(updatedsql)
  }
  
}

object AcumeCacheContext{
  
  def correctSQL(unparsedsql: String, parsedsql: Tuple2[List[Tuple], RequestType.RequestType]) = {
    
    val newunparsedsql = unparsedsql.replaceAll("\"","")
    val newparsedsql = (parsedsql._1.map(x => { 
      
      val tablename = x.getCubeName
      val newtablename = if(tablename.startsWith("\"") &&tablename.endsWith("\""))
        tablename.substring(1, tablename.length-1)
      else 
        tablename
      val newtuple = new Tuple()
      newtuple.setCubeName(newtablename)
      newtuple.setStartTime(x.getStartTime())
      newtuple.setEndTime(x.getEndTime())
      newtuple
    }), parsedsql._2)
    (newunparsedsql, newparsedsql)
  }
  
  def edit(parentExpression: Expression, expression: Expression): Boolean = {

    def checkNode(expression3: Expression) = {
      if (expression3.isInstanceOf[EqualsTo]) {
        val e1 = expression3.asInstanceOf[EqualsTo]
        val e2 = e1.getLeftExpression
        val e3 = e1.getRightExpression
        if (e2.isInstanceOf[Column] && e2.asInstanceOf[Column].getColumnName.equalsIgnoreCase("binsource") ||
          e3.isInstanceOf[Column] && e3.asInstanceOf[Column].getColumnName.equalsIgnoreCase("binsource")) {
          true
        } else {
          false
        }
      } else false
    }
    if (expression.isInstanceOf[Parenthesis]) {
      val childExpression = expression.asInstanceOf[Parenthesis].getExpression
      
      if(checkNode(expression.asInstanceOf[Parenthesis].getExpression)) {
        if(parentExpression.isInstanceOf[AndExpression]) {
          val parentE = parentExpression.asInstanceOf[AndExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(childExpression)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(childExpression)
          }
        }
        else if(parentExpression.isInstanceOf[OrExpression]) {
          val parentE = parentExpression.asInstanceOf[OrExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(childExpression)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(childExpression)
          }
        }
        else if(parentExpression.isInstanceOf[Parenthesis]) {
          parentExpression.asInstanceOf[Parenthesis].setExpression(expression)
        }
      }
      edit(expression, childExpression)
      false
    } else if (expression.isInstanceOf[AndExpression]) {
      val andE = expression.asInstanceOf[AndExpression]
      val leftE = andE.getLeftExpression
      val rightE = andE.getRightExpression
      
      if(checkNode(leftE)) { 
        
        if(parentExpression.isInstanceOf[AndExpression]) {
          val parentE = parentExpression.asInstanceOf[AndExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(rightE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(rightE)
          }
        }
        else if(parentExpression.isInstanceOf[OrExpression]) {
          val parentE = parentExpression.asInstanceOf[OrExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(rightE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(rightE)
          }
        }
        else if(parentExpression.isInstanceOf[Parenthesis]) {
          parentExpression.asInstanceOf[Parenthesis].setExpression(rightE)
        }
      }
      if(checkNode(rightE)) { 
        
        if(parentExpression.isInstanceOf[AndExpression]) {
          val parentE = parentExpression.asInstanceOf[AndExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(leftE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(leftE)
          }
        }
        else if(parentExpression.isInstanceOf[OrExpression]) {
          val parentE = parentExpression.asInstanceOf[OrExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(leftE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(leftE)
          }
        }
        else if(parentExpression.isInstanceOf[Parenthesis]) {
          parentExpression.asInstanceOf[Parenthesis].setExpression(leftE)
        }
      }
      
      edit(expression, andE.getLeftExpression)
      edit(expression, andE.getRightExpression)
        
      false
    } else if (expression.isInstanceOf[OrExpression]) {
      val orE = expression.asInstanceOf[OrExpression]
      val leftE = orE.getLeftExpression
      val rightE = orE.getRightExpression
      
      if(checkNode(leftE)) { 
        
        if(parentExpression.isInstanceOf[AndExpression]) {
          val parentE = parentExpression.asInstanceOf[AndExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(rightE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(rightE)
          }
        }
        else if(parentExpression.isInstanceOf[OrExpression]) {
          val parentE = parentExpression.asInstanceOf[OrExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(rightE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(rightE)
          }
        }
      }
      if(checkNode(rightE)) { 
        
        if(parentExpression.isInstanceOf[AndExpression]) {
          val parentE = parentExpression.asInstanceOf[AndExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(leftE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(leftE)
          }
        }
        else if(parentExpression.isInstanceOf[OrExpression]) {
          val parentE = parentExpression.asInstanceOf[OrExpression]
          if(parentE.getLeftExpression == expression) {
            parentE.setLeftExpression(leftE)
          }
          else if(parentE.getRightExpression == expression) {
            parentE.setRightExpression(leftE)
          }
        }
      }
      edit(expression, orE.getLeftExpression)
      edit(expression, orE.getRightExpression)
      false
    } 
    false
    
  }
  
  private [cache] def getTable(cube: String) = cube + "_" + getUniqueRandomNo 	
  
  private [cache] def getUniqueRandomNo: String = System.currentTimeMillis() + "" + Math.abs(new Random().nextInt())
  
  private def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf(AcumeConstants.TRIPLE_DOLLAR_SSC) + 1)
    
  def main(args: Array[String]) { 
    
    val sql = "Select * from x where (binsource = 10 and xz=42) or y=z and fkd>10 and dg>24"
    val sql1 = SQLParserFactory.getParserManager()
    val statement = sql1.parse(new StringReader(sql));
    edit(null, statement.asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect].getWhere)
    
  }
  
  private [workflow] def parseSql(sql: String) = { 
    
    val util = new SQLUtility();
    val list = util.getList(sql);
    val requestType = util.getRequestType(sql);
    (list, RequestType.getRequestType(requestType))
  }
  
  private[cache] def ACQL(sqlContext: SQLContext) = { 
    sqlContext.sql(_)
  }
}
