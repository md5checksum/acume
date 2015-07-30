package com.guavus.acume.cache.workflow

import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.utility.Utility

class AcumeHbaseCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) extends AcumeCacheContextTrait {

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
  initHbase
  
  private def constructQueryFromCube(cube: Cube) : String = {
    
    var queryString = new StringBuilder()
    queryString.append("create table ")
    queryString.append(cube.cubeName)
    queryString.append(" ( ")
    
    val fields = cube.dimension.dimensionSet.map(dimension => dimension.getName + " " + dimension.getDataType.typeString).toArray.mkString(", ")
    queryString.append(fields)
    
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
    cubeList.filter(cube => cube.dataSourceName.toLowerCase.startsWith("hbase")).map(cube => {
    	val query = constructQueryFromCube(cube)
      sqlContext.sql(query).collect
    })
  }
  
  private [acume] def cacheSqlContext() : SQLContext = sqlContext
  
  private [acume] def cacheConf = conf
  
  override val dataLoader = throw new RuntimeException("Operation not supported")
  
  override private [acume] def executeQuery(sql: String) = {
    val resultSchemaRDD = sqlContext.sql(sql)
    new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD.rdd, MetaData(-1, Nil))
  }

  private [acume] def getCubeMap = throw new RuntimeException("Operation not supported")
  
}