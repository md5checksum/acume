package com.guavus.acume.cache.workflow

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.immutable.SortedMap

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.core.CacheTimeSeriesLevelPolicy
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.disk.utility.InstaDataLoaderThinAcume
import com.guavus.acume.cache.sql.ISqlCorrector
import com.guavus.acume.cache.utility.Utility

/**
 * @author kashish.jain
 *
 */
class AcumeHiveCacheContext(override val cacheSqlContext: SQLContext, override val cacheConf: AcumeCacheConf) extends AcumeCacheContextTrait { 
 
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeHiveCacheContext])
  private [cache] val cacheTimeseriesLevelPolicy = new CacheTimeSeriesLevelPolicy(SortedMap[Long, Int]()(implicitly[Ordering[Long]].reverse) ++ Utility.getLevelPointMap(cacheConf.get(ConfConstants.acumecoretimeserieslevelmap)).map(x=> (x._1.level, x._2)))
  private val useInsta : Boolean = cacheConf.getBoolean(ConfConstants.useInsta).getOrElse(false)
  
  cacheSqlContext match {
    case hiveContext: HiveContext =>
    case hbaseContext : HBaseSQLContext =>
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  Utility.unmarshalXML(cacheConf.get(ConfConstants.businesscubexml), dimensionMap, measureMap)

  override val dataLoader : DataLoader = {
    if(!useInsta)
      null
    else
      new InstaDataLoaderThinAcume(this, cacheConf, null)
  } 
    
  override def getFirstBinPersistedTime(binSource: String): Long = {
    if(!useInsta)
      throw new RuntimeException("Operation not supported")
    else
      dataLoader.getFirstBinPersistedTime(binSource)
  }

  override def getLastBinPersistedTime(binSource: String): Long = {
    if(!useInsta)
      throw new RuntimeException("Operation not supported")
    else
      dataLoader.getLastBinPersistedTime(binSource)
  }

  override def getBinSourceToIntervalMap(binSource: String): Map[Long, (Long, Long)] = {
    if(!useInsta)
      throw new RuntimeException("Operation not supported")
    else
      dataLoader.getBinSourceToIntervalMap(binSource)
  }
  
  override def getAllBinSourceToIntervalMap() : Map[String, Map[Long, (Long,Long)]] =  {
    if(!useInsta)
      throw new RuntimeException("Operation not supported")
    else
		  dataLoader.getAllBinSourceToIntervalMap
  }

  override private[acume] def executeQuery(sql: String) = {
    if (!useInsta) {
      val resultSchemaRdd = cacheSqlContext.sql(sql)
      new AcumeCacheResponse(resultSchemaRdd, resultSchemaRdd.rdd, new MetaData(-1, Nil))
    } else {
      val originalparsedsql = AcumeCacheContext.parseSql(sql)

      println("AcumeRequest obtained " + sql)
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

        val key_binsource =
          if (binsource != null)
            binsource
          else
            cacheConf.get(ConfConstants.acumecorebinsource)

        i = AcumeCacheContext.getTable(cube)
        updatedsql = updatedsql.replaceAll(s"$cube", s"$i")
        val finalRdd = if (rt == RequestType.Timeseries) {
          val level =
            if (queryOptionalParams.getTimeSeriesGranularity() != 0) {
              queryOptionalParams.getTimeSeriesGranularity()
            } else
              cacheTimeseriesLevelPolicy.getLevelToUse(startTime, endTime, getLastBinPersistedTime(key_binsource))

          val startTimeCeiling = Utility.floorFromGranularity(startTime, level)
          val endTimeFloor = Utility.floorFromGranularity(endTime, level)
          timestamps = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
          val tables = for (timestamp <- timestamps) yield {
            val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null, null, null), timestamp, Utility.getNextTimeFromGranularity(timestamp, level, Utility.newCalendar), level)
            val tempTable = AcumeCacheContext.getTable(cube)
            rdd.registerTempTable(tempTable)
            val tempTable1 = AcumeCacheContext.getTable(cube)
            cacheSqlContext.sql(s"select *, $timestamp as ts from $tempTable").registerTempTable(tempTable1)
            tempTable1
          }
          val finalQuery = tables.map(x => s" select * from $x ").mkString(" union all ")
          cacheSqlContext.sql(finalQuery)
        } else {
          val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null, null, null), startTime, endTime, 0l)
          val tempTable = AcumeCacheContext.getTable(cube)
          rdd.registerTempTable(tempTable)
          cacheSqlContext.sql(s"select *, $startTime as ts from $tempTable")
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
