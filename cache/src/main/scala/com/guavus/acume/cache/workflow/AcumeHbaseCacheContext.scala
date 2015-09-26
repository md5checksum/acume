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
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.acume.cache.utility.Utility
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.utility.SQLParserFactory
import java.io.StringReader
import com.guavus.acume.cache.disk.utility.InstaDataLoaderThinAcume
import com.guavus.acume.cache.disk.utility.DataLoader

/**
 * @author kashish.jain
 *
 */
class AcumeHbaseCacheContext(cacheSqlContext: SQLContext, cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait(cacheSqlContext, cacheConf) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeHbaseCacheContext])
  private val useInsta : Boolean = cacheConf.getBoolean(ConfConstants.useInsta).getOrElse(false)
  
  override val dataLoader : DataLoader = {
    if(!useInsta)
      null
    else
      new InstaDataLoaderThinAcume(this, cacheConf, null)
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
    
    queryString.append(cube.hbaseConfigs.nameSpace + ".")
    
    queryString.append(cube.hbaseConfigs.tableName)
    
    queryString.append(", COLS=[")
    
    val columnMappings = cube.hbaseConfigs.columnMappings.map(mapping => mapping._1 + "=" + mapping._2).toArray.mkString(", ")
    queryString.append(columnMappings)
    
    queryString.append("])")
    
    logger.info("Firing query on Hbase: " + queryString.toString)
    queryString.toString
  }
  
  private def initHbase {

    if (!useInsta) {
      // Create table for every cube of hbase
      cubeList.map(cube => {
        val query = constructQueryFromCube(cube)
        val tableName = cube.cubeName

        //Drop table if already exists
        try {
          cacheSqlContext.sql(s"drop table $tableName").collect
          logger.info(s"temp table $tableName dropped")
        } catch {
          case e: Exception => logger.error(s"Dropping temp table $tableName failed. ", e.getLocalizedMessage)
          case th: Throwable => logger.error(s"Dropping temp table $tableName failed. ", th.getLocalizedMessage)
        }

        //Create table with cubename
        try {
          cacheSqlContext.sql(query).collect
          logger.info(s"temp table $tableName created")
        } catch {
          case e: Exception => throw new RuntimeException(s"Creating temp table $tableName failed. ", e)
          case th: Throwable => throw new RuntimeException(s"Creating temp table $tableName failed. ", th)
        }
      })
    }
  }
  
  override private [acume] def executeQuery(sql: String) = {
    logger.info("AcumeRequest obtained on HBASE: " + sql)

    var (timestamps, correctsql, level) = getTimestampsAndSql(sql)

    var updatedsql = correctsql._1._1
    val updatedparsedsql = correctsql._2
    val cube = updatedparsedsql._1(0).getCubeName
    val binsource = updatedparsedsql._1(0).getBinsource
    val rt = updatedparsedsql._2
    val startTime = updatedparsedsql._1(0).getStartTime
    val endTime = updatedparsedsql._1(0).getEndTime
    
    if (!useInsta) {
      executeThinClientQuery(updatedsql, timestamps)
      } else {
      var tableName = AcumeCacheContextTraitUtil.getTable(cube)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$tableName")
      executeThickClientQuery(updatedsql, timestamps, cube, binsource, rt, startTime, endTime, level, tableName)
    }
  }
  
}