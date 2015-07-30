package com.guavus.acume.cache.workflow

import java.io.StringReader
import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.core.AcumeCacheFactory
import com.guavus.acume.cache.core.CacheIdentifier
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.sql.ISqlCorrector
import com.guavus.acume.cache.utility.SQLParserFactory
import com.guavus.acume.cache.utility.SQLUtility
import com.guavus.acume.cache.utility.Tuple
import com.guavus.acume.cache.utility.Utility
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.Parenthesis
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Select
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
 * @author archit.thakur
 *
 */
class AcumeCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) extends AcumeCacheContextTrait {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeCacheContext])
  
  private [cache] val baseCubeList = MutableList[BaseCube]()
  private [cache] val cubeMap = new HashMap[CubeKey, Cube]
  private [cache] val cubeList = MutableList[Cube]()

  sqlContext match {
    case hiveContext: HiveContext =>
    case hbaseContext : HBaseSQLContext =>
    case sqlContext: SQLContext => 
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  Utility.init(conf)
  Utility.loadXML(conf, dimensionMap, measureMap, cubeMap, cubeList)

  private [acume] def cacheConf() = conf
  
  private [acume] def cacheSqlContext() = sqlContext

  override val dataLoader = DataLoader.getDataLoader(this, conf, null)
  
  override def getCubeMap = cubeMap.toMap
  
  override def getFirstBinPersistedTime(binSource: String): Long = {
    dataLoader.getFirstBinPersistedTime(binSource)
  }

  override def getLastBinPersistedTime(binSource: String): Long = {
    dataLoader.getLastBinPersistedTime(binSource)
  }

  override def getBinSourceToIntervalMap(binSource: String): Map[Long, (Long, Long)] = {
    dataLoader.getBinSourceToIntervalMap(binSource)
  }
  
  override def getAllBinSourceToIntervalMap() : Map[String, Map[Long, (Long,Long)]] =  {
		dataLoader.getAllBinSourceToIntervalMap
  }
  
  override private [acume] def getCubeList = cubeList.toList
   
  override private [acume] def executeQuery(sql: String) = {
    
    val originalparsedsql = AcumeCacheContext.parseSql(sql)
    
    logger.info("AcumeRequest obtained " + sql)
    var correctsql = ISqlCorrector.getSQLCorrector(conf).correctSQL(this, sql, (originalparsedsql._1.toList, originalparsedsql._2))
    var updatedsql = correctsql._1._1
    val queryOptionalParams = correctsql._1._2
    var updatedparsedsql = correctsql._2
    
    val rt = updatedparsedsql._2
      
    var i = ""
    var cubes = new ArrayBuffer[Cube]
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
        conf.get(ConfConstants.acumecorebinsource)

      i = AcumeCacheContext.getTable(cube)
      val id = getCube(CubeKey(cube, key_binsource))
      cubes.+=(id)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$i")
      val idd = new CacheIdentifier()
      idd.put("cube", id.hashCode)
      val instance = AcumeCacheFactory.getInstance(this, conf, idd, id)
      if(l.getSingleEntityKeyValueList() == null  || l.getSingleEntityKeyValueList().size == 0) {
    	  instance.createTempTableAndMetadata(List(Map[String, Any]()), startTime, endTime, rt, i,Some(queryOptionalParams))
      } else {
        val singleEntityKeys = (for(singleEntityKeys <- l.getSingleEntityKeyValueList()) yield {
          singleEntityKeys.map(x => (x._1 -> x._2.asInstanceOf[Any])).toMap
        }).toList
        instance.createTempTableAndMetadata(singleEntityKeys, startTime, endTime, rt, i,Some(queryOptionalParams))
      }
    }
    
    val klist = list.flatMap(_.timestamps).toList
    val kfg = execute(cubes.toList, updatedsql)
    AcumeCacheResponse(kfg, kfg.rdd, MetaData(-1, klist))
  }
  
  private [acume] def validateQuery(startTime : Long, endTime : Long, binSource : String) {
    if(startTime < getFirstBinPersistedTime(binSource) || endTime > getLastBinPersistedTime(binSource)){
      throw new RuntimeException("Cannot serve query. StartTime and endTime doesn't fall in the availability range.")
    }
  }
  
  private [acume] def execute(cubes :List[Cube], updatedsql: String) = {
    AcumeCacheContext.ACQL(sqlContext)(updatedsql)
  }
  
  override private [acume] def getFieldsForCube(name: String, binsource: String) = {
      
    val cube = cubeMap.getOrElse(CubeKey(name, binsource), throw new RuntimeException(s"Cube $name Not in AcumeCache knowledge."))
    cube.dimension.dimensionSet.map(_.getName) ++ cube.measure.measureSet.map(_.getName)
  }
  
  override private [acume] def getAggregationFunction(stringname: String) = {
    val measure = measureMap.getOrElse(stringname, throw new RuntimeException(s"Measure $stringname not in Acume knowledge."))
    measure.getAggregationFunction
  }
  
  override private [acume] def getCubeListContainingFields(lstfieldNames: List[String]) = {
    
    val dimensionSet = scala.collection.mutable.Set[Dimension]()
    val measureSet = scala.collection.mutable.Set[Measure]()
    for(field <- lstfieldNames)
      if(isDimension(field))
        dimensionSet.+=(dimensionMap.get(field).get)
      else
        measureSet.+=(measureMap.get(field).get)
      val kCube = 
        for(cube <- cubeList if(dimensionSet.toSet.subsetOf(cube.dimension.dimensionSet.toSet) && 
            measureSet.toSet.subsetOf(cube.measure.measureSet.toSet))) yield {
          cube
        }
    kCube.toList
  }
  
  private [cache] def getCube(cube: CubeKey) = cubeMap.get(cube).getOrElse(throw new RuntimeException(s"cube $cube not found."))
  
  private [workflow] def loadBaseXML(filedir: String) = {
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
