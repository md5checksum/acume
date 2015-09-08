package com.guavus.acume.cache.workflow

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.sql.SQLContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.disk.utility.InstaDataLoaderThinAcume
import com.guavus.acume.cache.sql.ISqlCorrector
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
    
    if (!useInsta) {
      val resultSchemaRdd = cacheSqlContext.sql(sql)
      new AcumeCacheResponse(resultSchemaRdd, resultSchemaRdd.rdd, new MetaData(-1, Nil))
    } else {
      val originalparsedsql = AcumeCacheContextTraitUtil.parseSql(sql)

      var correctsql = ISqlCorrector.getSQLCorrector(cacheConf).correctSQL(this, sql, (originalparsedsql._1.toList, originalparsedsql._2))
      var updatedsql = correctsql._1._1
      val queryOptionalParams = correctsql._1._2
      var updatedparsedsql = correctsql._2
      val rt = updatedparsedsql._2

      var i = ""
      var timestamps = scala.collection.mutable.MutableList[Long]()
      
      val list = for (l <- updatedparsedsql._1) yield {
        val cube = l.getCubeName
        val binsource = l.getBinsource
        val startTime = l.getStartTime
        val endTime = l.getEndTime

        i = AcumeCacheContextTraitUtil.getTable(cube)
        updatedsql = updatedsql.replaceAll(s"$cube", s"$i")

        val finalRdd = if (rt == RequestType.Timeseries) {
          
          val level =
            if (queryOptionalParams.getTimeSeriesGranularity() != 0) {
              queryOptionalParams.getTimeSeriesGranularity()
            } else
              cacheTimeseriesLevelPolicy.getLevelToUse(startTime, endTime, BinAvailabilityPoller.getLastBinPersistedTime(binsource))

          val startTimeCeiling = Utility.floorFromGranularity(startTime, level)
          val endTimeFloor = Utility.floorFromGranularity(endTime, level)
          timestamps = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
          
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
        print("Registering Temp Table " + i)
        finalRdd.registerTempTable(i)
      }
      print("Thin Query " + updatedsql)
      val resultSchemaRDD = cacheSqlContext.sql(updatedsql)
      new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD.rdd, MetaData(-1, timestamps.toList))
    }
  }
  
}
