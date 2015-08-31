package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.sql.ISqlCorrector
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
 * @author kashish.jain
 *
 */
class AcumeHbaseCacheContext(cacheSqlContext: SQLContext, cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait(cacheSqlContext, cacheConf) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeHbaseCacheContext])
   
  initHbase
  
  private def constructQueryFromCube(cube: Cube) : String = {
    
    var queryString = new StringBuilder()
    queryString.append("create table ")
    queryString.append(cube.cubeName)
    queryString.append(" ( ")
    
    val dimFields = cube.dimension.dimensionSet.map(dimension => dimension.getName + " " + ConversionToSpark.convertToSparkDataType(dimension.getDataType).typeName).toArray.mkString(", ")
    queryString.append(dimFields)
    
    val measureFields = cube.measure.measureSet.map(measure => measure.getName + " " + ConversionToSpark.convertToSparkDataType(measure.getDataType).typeName).toArray.mkString(", ")
    queryString.append(", " + measureFields)
    
    queryString.append(", primary key(")
    val primaryKeys = cube.hbaseConfigs.primaryKeys.mkString(",")
    queryString.append(primaryKeys)
    queryString.append(")) mapped by (")
    
    queryString.append(cube.hbaseConfigs.tableName)
    
    queryString.append(", COLS=[")
    
    val columnMappings = cube.hbaseConfigs.columnMappings.map(mapping => mapping._1 + "=" + mapping._2).toArray.mkString(", ")
    queryString.append(columnMappings)
    
    queryString.append("])")
    
    logger.info("Firing query on Hbase: " + queryString.toString)
    queryString.toString
  }
  
  private def initHbase {
    // Create table for every cube of hbase
    cubeList.map(cube => {
    	val query = constructQueryFromCube(cube)
      val cubeName = cube.cubeName
      
      //Drop table if already exists
      try{
    	  cacheSqlContext.sql("drop table " + cubeName).collect
        logger.info(s"temp table $cubeName dropped")
      } catch {
        case e: Exception => logger.error(s"Dropping temp table $cubeName failed. ", e)
        case th : Throwable => logger.error(s"Dropping temp table $cubeName failed. ", th)
      }
      
      //Create table with cubename
      try{
        cacheSqlContext.sql(query).collect
        logger.info(s"temp table $cubeName created")
      } catch {
        case e: Exception => throw new RuntimeException(s"Creating temp table $cubeName failed. " , e)
        case th : Throwable => throw new RuntimeException(s"Creating temp table $cubeName failed. ", th)
      }
    })
  }
  
  override private [acume] def executeQuery(sql: String) = {
    val originalparsedsql = AcumeCacheContextTraitUtil.parseSql(sql)
    println("AcumeRequest obtained on HBASE: " + sql)
    
    var correctsql = ISqlCorrector.getSQLCorrector(cacheConf).correctSQL(this, sql, (originalparsedsql._1.toList, originalparsedsql._2))
    
    var updatedsql = correctsql._1._1
    var updatedparsedsql = correctsql._2
  
    val l = updatedparsedsql._1(0)
    val binsource = l.getBinsource
    val startTime = l.getStartTime
    val endTime = l.getEndTime
    
    AcumeCacheContextTraitUtil.validateQuery(startTime, endTime, binsource)
    
    logger.info("Firing corrected query on HBASE " +  updatedsql)
    val resultSchemaRDD = cacheSqlContext.sql(updatedsql)
    new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD.rdd, MetaData(-1, Nil))
  }
  
}