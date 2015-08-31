package com.guavus.acume.cache.workflow

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.utility.InsensitiveStringKeyHashMap
import com.guavus.acume.cache.utility.Utility
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.Parenthesis
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import com.guavus.acume.cache.utility.SQLUtility
import java.util.Random
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller

/**
 * @author kashish.jain
 */
object AcumeCacheContextTraitUtil {
  
  val dimensionMap = new InsensitiveStringKeyHashMap[Dimension]
  val measureMap = new InsensitiveStringKeyHashMap[Measure]
  val cubeMap = new HashMap[CubeKey, Cube]
  val cubeList = MutableList[Cube]()
  val cacheConf = new AcumeCacheConf
  val dataloadermap : ConcurrentHashMap[String, DataLoader] = new ConcurrentHashMap[String, DataLoader]

  private val inheritablePoolThreadLocal = new InheritableThreadLocal[HashMap[String, Any]]()
  
  /*
   * Initializing and reading acume's cubedefiniton.xml
   * 
   */
  Utility.init(cacheConf)
  Utility.loadXML(cacheConf, dimensionMap, measureMap, cubeMap, cubeList)

  
  /*
   * Setting pool level threadlocal params
   */
  def poolThreadLocal: InheritableThreadLocal[HashMap[String, Any]] = inheritablePoolThreadLocal
  

  /*
   * Setting threadLocal params for query execution
   */
  private val threadLocal = new ThreadLocal[HashMap[String, Any]]() { 
    override protected def initialValue() : HashMap[String, Any] = {
      new HashMap[String, Any]()
    }
  }
  
  def setQuery(query : String) {
    threadLocal.get.put("query", query)
  }
  
  def getQuery(): String = {
    return threadLocal.get.getOrElse("query", null).asInstanceOf[String]
  }
  
  def unsetQuery() {
    threadLocal.get.put("query", null)
  }
  
  def addAcumeTreeCacheValue(acumeTreeCacheValue : AcumeTreeCacheValue) {
    val list = threadLocal.get.getOrElse("AcumeTreeCacheValue", new ArrayBuffer[AcumeTreeCacheValue]).asInstanceOf[ArrayBuffer[AcumeTreeCacheValue]]
    list.+=(acumeTreeCacheValue)
    threadLocal.get().put("AcumeTreeCacheValue", list)
  }
  
  def setQueryTable(tableName : String) {
    val list = threadLocal.get.getOrElse("QueryTable", new ArrayBuffer[String]).asInstanceOf[ArrayBuffer[String]]
    list.+=(tableName)
    threadLocal.get().put("QueryTable", list)
  }
  
  def setInstaTempTable(tableName : String) {
    val list = threadLocal.get.getOrElse("InstaTempTable", new ArrayBuffer[String]).asInstanceOf[ArrayBuffer[String]]
    list.+=(tableName)
    threadLocal.get().put("InstaTempTable", list)
  }
  
  def setSparkSqlShufflePartitions(numPartitions: String) {
    threadLocal.get.put(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, numPartitions)
  }
  
  def getSparkSqlShufflePartitions(): String = {
    threadLocal.get.get(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS).get.asInstanceOf[String]
  }

  def unsetAcumeTreeCacheValue() {
    threadLocal.get.remove("AcumeTreeCacheValue")
  }
  
  def unsetQueryTable(cacheContext : AcumeCacheContextTrait) {
    val x = threadLocal.get.remove("QueryTable").map(x => {
      x.asInstanceOf[ArrayBuffer[String]].map(cacheContext.cacheSqlContext.dropTempTable(_))
    })
  }
  
  def getInstaTempTable() = {
    threadLocal.get.remove("InstaTempTable")
  }
  
  def unsetAll(cacheContext : AcumeCacheContextTrait) {
    unsetQueryTable(cacheContext)
    getInstaTempTable
    unsetAcumeTreeCacheValue
  }
  
  /*
   * Common functionalities to be used by all acumeCacheContextTraits
   */
  
  private def edit(parentExpression: Expression, expression: Expression): Boolean = {

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
  
  private [workflow] def parseSql(sql: String) = { 
    
    val util = new SQLUtility();
    val list = util.getList(sql);
    val requestType = util.getRequestType(sql);
    (list, RequestType.getRequestType(requestType))
  }
  
  private [acume] def validateQuery(startTime : Long, endTime : Long, binSource : String, dsName: String) {
    if(startTime < BinAvailabilityPoller.getFirstBinPersistedTime(binSource) || endTime > BinAvailabilityPoller.getLastBinPersistedTime(binSource)){
      throw new RuntimeException("Cannot serve query. StartTime and endTime doesn't fall in the availability range.")
    }
    
    val numberOfCubes = AcumeCacheContextTraitUtil.cubeList.filter(cube => cube.dataSource.equalsIgnoreCase(dsName)).filter(cube => cube.binSource.equalsIgnoreCase(binSource)).size
    if(numberOfCubes == 0)
      throw new RuntimeException(s"The binsource $binSource does not belong to the datasource $dsName")    
      
  }
  
}