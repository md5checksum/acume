package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube

class AcumeHbaseCacheContext(override val cacheSqlContext: SQLContext, override val cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeHbaseCacheContext])
   
  cacheSqlContext match {
    case hiveContext: HiveContext =>
    case hbaseContext : HBaseSQLContext =>
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
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
    
    queryString.toString
  }
  
  private def initHbase {
    // Create table for every cube of hbase
    cubeList.map(cube => {
    	val query = constructQueryFromCube(cube)
      //Drop table if already exists
      try{
    	  cacheSqlContext.sql("drop table " + cube.cubeName).collect
      } catch {
        case e: Exception => println(e.getMessage)
        case th : Throwable => println(th.getMessage)
      }
      
      //Create table with cubename
      try{
        cacheSqlContext.sql(query).collect
      } catch {
        case e: Exception => println(e.getMessage)
        case th : Throwable => println(th.getMessage)
      }
    })
  }
  
  override private [acume] def executeQuery(sql: String) = {
    val resultSchemaRDD = cacheSqlContext.sql(sql)
    new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD.rdd, MetaData(-1, Nil))
  }
  
}