package com.guavus.acume.cache.sql

import com.guavus.acume.cache.utility.Tuple
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.workflow.RequestType._
import net.sf.jsqlparser.expression.operators.conditional._
import net.sf.jsqlparser.expression.operators.relational._
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.schema.Column
import scala.reflect.internal.Types
import com.guavus.acume.cache.utility.SQLParserFactory
import net.sf.jsqlparser.statement.select.Select
import java.io.StringReader
import net.sf.jsqlparser.statement.select.PlainSelect
import scala.collection.JavaConversions._
import com.guavus.acume.cache.common.Cube
import java.util.{LinkedList => JLinkedList}
import java.util.{HashMap => JHashMap }
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.workflow.CubeKey
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil

/**
 * @author archit.thakur
 *
 */

object AcumeCacheSQLCorrector {
  def main(args: Array[String]) {
    val queryoptionalParams = new QueryOptionalParam()
    val sql = List("Select * from x where (binsource = 10 and xz=42) or y=z and fkd>10 and dg>24",
        "Select * from x where (binsource = 10 and xz=42 or y=z) and fkd>10 and dg>24",
        "Select * from x where binsource = 10 and (xz=42 or y=z) and fkd>10 and dg>24",
        "Select * from x where (binsource = 10 and (xz=42 or y=z)) and fkd>10 and dg>24",
        "Select * from x where (binsource = 10 and (xz=42) or y=z) and fkd>10 and dg>24", 
      "Select * from x where (binsource = 10)",
      "Select * from x where binsource = 10",
      "select HIT_COUNT_TEMP from searchEgressPeerCube where ts >=1404723600 and ts <1404727200 and x=1 and y=2 and z=3 and binsource = 10")
    val sql1 = SQLParserFactory.getParserManager()
    val statement = sql.map(x => sql1.parse(new StringReader(x)))
    val j123 = new AcumeCacheSQLCorrector(new AcumeCacheConf)
    statement.map(y => {
      val ex1 = AcumeCacheCorrectorExpression(y.asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect].getWhere)
      val ex2 = AcumeCacheCorrectorExpression(y.asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect].getWhere)
      println(y.asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect].toString)
      println("original => " + ex1)
      j123.edit(ex1, ex2, queryoptionalParams)
      println(ex1)
    })
  }
}

case class AcumeCacheCorrectorExpression(var expression: Expression)

class AcumeCacheSQLCorrector(val conf: AcumeCacheConf) extends ISqlCorrector {

  override def correctSQL(acumeCacheContextTrait: AcumeCacheContextTrait, unparsedsql: String, parsedsql: Tuple2[List[Tuple], RequestType]): ((String, QueryOptionalParam), (List[Tuple], RequestType)) = {
	  val queryoptionalParams = new QueryOptionalParam()

    val sql = SQLParserFactory.getParserManager()
    val sql_statement = sql.parse(new StringReader(unparsedsql))
    val plainselect = sql_statement.asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val sql_where = plainselect.getWhere
    val expression = AcumeCacheCorrectorExpression(sql_where)
    
    edit(expression, expression, queryoptionalParams)
    //Temporary hack in case a single query contains both binsource and rubix_cache_compression_interval
    //TODO: Fix me generically
    edit(expression, expression, queryoptionalParams)
    plainselect.setWhere(expression.expression)
    
    val newunparsedsql = plainselect.toString.replaceAll("\"", "")
    val newparsedsql = (parsedsql._1.map(x => {

      val tablename = x.getCubeName
      val newtablename = if (tablename.startsWith("\"") && tablename.endsWith("\""))
        tablename.substring(1, tablename.length - 1)
      else
        tablename
        
      val querybinsource = x.getBinsource

      val key_binsource = {
        if (querybinsource != null)
          querybinsource
        else {
          val eligibleCubes = AcumeCacheContextTraitUtil.cubeList.filter(x => x.cubeName.equalsIgnoreCase(tablename))
          if (eligibleCubes.size == 1)
            eligibleCubes.get(0).get.binSource
          else
            conf.get(ConfConstants.acumecorebinsource)

        }
      }
        
      val xlist = x.getSingleEntityKeyValueList
      val singleEntityKey = acumeCacheContextTrait.getCubeMap.getOrElse(CubeKey(newtablename, key_binsource), throw new RuntimeException("Cube not found")).singleEntityKeys
      if (singleEntityKey == null || singleEntityKey.isEmpty) {
        x.setSingleEntityKeyValueList(new java.util.LinkedList[java.util.HashMap[String, Object]])
      } else {
        xlist.map(y => {
          for (ix <- y.entrySet()) {
            if (!singleEntityKey.contains(ix.getKey))
              y.remove(ix.getKey)
          }
          y
        })
      }
      val newtuple = new Tuple()
      newtuple.set(x.getStartTime, x.getEndTime, newtablename, key_binsource, x.getSingleEntityKeyValueList)
      newtuple
    }), parsedsql._2)
    ((newunparsedsql, queryoptionalParams), newparsedsql)
  }

  def edit(parentExpression: AcumeCacheCorrectorExpression, expression: AcumeCacheCorrectorExpression, queryoptionalParams : QueryOptionalParam): Boolean = {

    def checkNode(expression3: Expression) = {
      if (expression3.isInstanceOf[EqualsTo]) {
        val e1 = expression3.asInstanceOf[EqualsTo]
        val e2 = e1.getLeftExpression
        val e3 = e1.getRightExpression
        if (e2.isInstanceOf[Column] && e2.asInstanceOf[Column].getColumnName.equalsIgnoreCase("binsource") ||
          e3.isInstanceOf[Column] && e3.asInstanceOf[Column].getColumnName.equalsIgnoreCase("binsource")) {
          true
        } else if(e2.isInstanceOf[Column] && e2.asInstanceOf[Column].getColumnName.equalsIgnoreCase("RUBIX_CACHE_COMPRESSION_INTERVAL")){
          queryoptionalParams.setTimeSeriesGranularity(e3.asInstanceOf[StringValue].getValue.toLong)
          true
        } 
        else if(e3.isInstanceOf[Column] && e3.asInstanceOf[Column].getColumnName.equalsIgnoreCase("RUBIX_CACHE_COMPRESSION_INTERVAL")) {
          queryoptionalParams.setTimeSeriesGranularity(e2.asInstanceOf[StringValue].getValue.toLong)
          true
        } else { 
          false
        }
      } else false
    }
    if((parentExpression.expression == expression.expression || 
        (parentExpression.expression.isInstanceOf[Parenthesis] && parentExpression.expression.asInstanceOf[Parenthesis].getExpression == expression.expression))
        && checkNode(expression.expression)) {
      parentExpression.expression = null
      expression.expression = null
//      edit(expression, AcumeCacheCorrectorExpression(expression.expression.asInstanceOf[Parenthesis].getExpression))
    }
    else if (expression.expression.isInstanceOf[Parenthesis]) {
      val childExpression = expression.expression.asInstanceOf[Parenthesis].getExpression

      if (parentExpression.expression != expression.expression) {
        if (checkNode(expression.expression.asInstanceOf[Parenthesis].getExpression)) {
          if (parentExpression.expression.isInstanceOf[AndExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[AndExpression]
            if (parentE.getLeftExpression == expression.expression) {
              expression.expression = parentE.getRightExpression
            } else if (parentE.getRightExpression == expression.expression) {
              expression.expression = parentE.getLeftExpression
            }
          } else if (parentExpression.expression.isInstanceOf[OrExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[OrExpression]
            if (parentE.getLeftExpression == expression.expression) {
              expression.expression = parentE.getLeftExpression
            } else if (parentE.getRightExpression == expression.expression) {
              expression.expression = parentE.getRightExpression
            }
          } else if (parentExpression.expression.isInstanceOf[Parenthesis]) {
            expression.expression.asInstanceOf[Parenthesis].setExpression(expression.expression.asInstanceOf[Parenthesis].getExpression)
          }
          //        edit(parentExpression, AcumeCacheCorrectorExpression(expression))}
        }
      }
      edit(expression, AcumeCacheCorrectorExpression(expression.expression.asInstanceOf[Parenthesis].getExpression), queryoptionalParams)
      false

    } else if (expression.expression.isInstanceOf[AndExpression]) {
      val andE = expression.expression.asInstanceOf[AndExpression]
      val leftE = andE.getLeftExpression
      val rightE = andE.getRightExpression

      if (checkNode(leftE)) {

        if (parentExpression.expression != expression.expression) {

          if (parentExpression.expression.isInstanceOf[AndExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[AndExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(rightE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(rightE)
            }
          } else if (parentExpression.expression.isInstanceOf[OrExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[OrExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(rightE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(rightE)
            }
          } else if (parentExpression.expression.isInstanceOf[Parenthesis]) {
            parentExpression.expression.asInstanceOf[Parenthesis].setExpression(rightE)
          }
        } else {
          expression.expression = rightE
          parentExpression.expression = rightE
        }
      }

      if (checkNode(rightE)) {

        if (parentExpression.expression != expression.expression) {
          if (parentExpression.expression.isInstanceOf[AndExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[AndExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(leftE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(leftE)
            }
          } else if (parentExpression.expression.isInstanceOf[OrExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[OrExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(leftE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(leftE)
            }
          } else if (parentExpression.expression.isInstanceOf[Parenthesis]) {
            parentExpression.expression.asInstanceOf[Parenthesis].setExpression(leftE)
          }
        } else {
          expression.expression = leftE
          parentExpression.expression = leftE
        }
      }

      edit(expression, AcumeCacheCorrectorExpression(andE.getLeftExpression), queryoptionalParams)
      edit(expression, AcumeCacheCorrectorExpression(andE.getRightExpression), queryoptionalParams)

      false
    } else if (expression.expression.isInstanceOf[OrExpression]) {
      val orE = expression.expression.asInstanceOf[OrExpression]
      val leftE = orE.getLeftExpression
      val rightE = orE.getRightExpression

      if (checkNode(leftE)) {

        if (parentExpression.expression != expression.expression) {
          if (parentExpression.expression.isInstanceOf[AndExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[AndExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(rightE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(rightE)
            }
          } else if (parentExpression.expression.isInstanceOf[OrExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[OrExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(rightE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(rightE)
            }
          }
        } else {
          parentExpression.expression = rightE
          expression.expression = rightE
        }
      }

      if (checkNode(rightE)) {

        if (parentExpression.expression != expression.expression) {
          if (parentExpression.expression.isInstanceOf[AndExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[AndExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(leftE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(leftE)
            }
          } else if (parentExpression.expression.isInstanceOf[OrExpression]) {
            val parentE = parentExpression.expression.asInstanceOf[OrExpression]
            if (parentE.getLeftExpression == expression.expression) {
              parentE.setLeftExpression(leftE)
            } else if (parentE.getRightExpression == expression.expression) {
              parentE.setRightExpression(leftE)
            }
          }
        } else {
          parentExpression.expression = leftE
          expression.expression = leftE
        }
      }
      edit(expression, AcumeCacheCorrectorExpression(orE.getLeftExpression), queryoptionalParams)
      edit(expression, AcumeCacheCorrectorExpression(orE.getRightExpression), queryoptionalParams)
      false
    }
    false

  }
}