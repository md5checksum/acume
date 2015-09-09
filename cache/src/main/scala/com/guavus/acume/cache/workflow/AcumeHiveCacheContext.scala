package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.disk.utility.InstaDataLoaderThinAcume
import com.guavus.acume.cache.utility.Utility

/**
 * @author kashish.jain
 *
 */
class AcumeHiveCacheContext(cacheSqlContext: SQLContext, cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait(cacheSqlContext, cacheConf) { 
 
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeHiveCacheContext])
  private val useInsta : Boolean = cacheConf.getBoolean(ConfConstants.useInsta).getOrElse(false)
  
  override val dataLoader : DataLoader = {
    if(!useInsta)
      null
    else
      new InstaDataLoaderThinAcume(this, cacheConf, null)
  }

  override private[acume] def executeQuery(sql: String) = {

    logger.info("AcumeRequest obtained " + sql)

    var (timestamps, correctsql, level) = getTimestampsAndSql(sql)

    var updatedsql = correctsql._1._1
    val updatedparsedsql = correctsql._2
    val cube = updatedparsedsql._1(0).getCubeName
    val binsource = updatedparsedsql._1(0).getBinsource
    val rt = updatedparsedsql._2
    val startTime = updatedparsedsql._1(0).getStartTime
    val endTime = updatedparsedsql._1(0).getEndTime

    if (!useInsta) {
      // Firing on thin client
      //Replace the ts with "timestamp"
      val tsRegex = "\\b" + "ts" + "\\b"
      val tsReplacedSql = updatedsql.replaceAll(tsRegex, "timestamp")
      
      val resultSchemaRdd = cacheSqlContext.sql(tsReplacedSql)
      logger.info(s"Firing thin client Query $tsReplacedSql")
      new AcumeCacheResponse(resultSchemaRdd, resultSchemaRdd.rdd, new MetaData(-1, timestamps.toList))
    
    } else {
      // Firing on thick client
      var tableName = AcumeCacheContextTraitUtil.getTable(cube)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$tableName")

      val finalRdd = if (rt == RequestType.Timeseries) {
        val tables = for (timestamp <- timestamps) yield {
          
          val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null, null, null), timestamp, Utility.getNextTimeFromGranularity(timestamp, level, Utility.newCalendar), level)
          val tempTable = AcumeCacheContextTraitUtil.getTable(cube)
          rdd.registerTempTable(tempTable)
          val tempTable1 = AcumeCacheContextTraitUtil.getTable(cube)
          val tString = s"{timestamp}L"
          cacheSqlContext.sql(s"select *, $tString as ts from $tempTable").registerTempTable(tempTable1)
          tempTable1
        }

        val finalQuery = tables.map(x => s" select * from $x ").mkString(" union all ")
        cacheSqlContext.sql(finalQuery)

      } else {
        val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null, null, null), startTime, endTime, 0l)
        val tempTable = AcumeCacheContextTraitUtil.getTable(cube)
        rdd.registerTempTable(tempTable)
        val sString = s"{startTime}L"
        cacheSqlContext.sql(s"select *, $sString as ts from $tempTable")
      }
      
      logger.info(s"Registering Temp Table $tableName")
      finalRdd.registerTempTable(tableName)
      logger.info(s"Firing thick client Query $updatedsql")
      val resultSchemaRDD = cacheSqlContext.sql(updatedsql)
      new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD.rdd, MetaData(-1, timestamps.toList))
    }
  }
  
}
