package com.guavus.acume.cache.workflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.sql.ISqlCorrector
import scala.collection.mutable.ArrayBuffer
import com.guavus.acume.cache.common.Cube
import scala.collection.JavaConversions._
import com.guavus.acume.cache.disk.utility.DataLoader
import java.util.concurrent.ConcurrentHashMap
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.disk.utility.InstaDataLoaderThinAcume
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.core.FixedLevelPolicy
import com.guavus.acume.cache.core.CacheTimeSeriesLevelPolicy
import scala.collection.immutable.SortedMap
import com.guavus.acume.cache.utility.QueryOptionalParam
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.utility.Tuple
import com.guavus.acume.cache.utility.RequestType._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @author kashish.jain
 *
 */
class AcumeHiveCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) extends AcumeCacheContextTrait { 
 
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeHiveCacheContext])
  
  sqlContext match {
    case hiveContext: HiveContext =>
    case sqlContext: SQLContext => 
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  Utility.init(conf)
  Utility.loadXML(conf, dimensionMap, measureMap, cubeMap, cubeList)
  
  val dataLoader = new InstaDataLoaderThinAcume(this, conf, null)
  
  override private [cache] val dataloadermap = new ConcurrentHashMap[String, DataLoader]
  
  private [acume] def cacheSqlContext() : SQLContext = sqlContext
  
  private [acume] def cacheConf = conf
  
  private [acume] def getCubeMap = throw new RuntimeException("Operation not supported")
  
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
  
  val cacheTimeseriesLevelPolicy = new CacheTimeSeriesLevelPolicy(SortedMap[Long, Int]()(implicitly[Ordering[Long]]) ++ Utility.getLevelPointMap(conf.get(ConfConstants.acumecoretimeserieslevelmap)))

  private[acume] def executeQuery(sql: String, qltype: QLType.QLType) = {
    logger.info("AcumeRequest obtained " + sql)

    var (timestamps, correctsql, level) = getTimestampsAndSql(sql)

    var updatedsql = correctsql._1._1
    val updatedparsedsql = correctsql._2
    val cube = updatedparsedsql._1(0).getCubeName
    val binsource = updatedparsedsql._1(0).getBinsource
    val rt = updatedparsedsql._2
    val startTime = updatedparsedsql._1(0).getStartTime
    val endTime = updatedparsedsql._1(0).getEndTime

    if (!cacheConf.getBoolean(ConfConstants.useInsta, false)) {
      logger.info(s"Firing thin client Query $updatedsql")
      val resultSchemaRdd = sqlContext.sql(updatedsql)
      new AcumeCacheResponse(resultSchemaRdd, resultSchemaRdd, new MetaData(-1, timestamps.toList))

    } else {

      val tableName = AcumeCacheContext.getTable(cube)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$tableName")

      val finalRdd = if (rt == RequestType.Timeseries) {

        val tables = for (timestamp <- timestamps) yield {
          val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null), timestamp, Utility.getNextTimeFromGranularity(timestamp, level, Utility.newCalendar))
          val tempTable = AcumeCacheContext.getTable(cube)
          rdd.registerTempTable(tempTable)
          val tempTable1 = AcumeCacheContext.getTable(cube)
          sqlContext.sql(s"select *, $timestamp as ts from $tempTable").registerTempTable(tempTable1)
          tempTable1
        }

        val finalQuery = tables.map(x => s" select * from $x ").mkString(" union all ")
        sqlContext.sql(finalQuery)
      } else {
        val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null), startTime, endTime)
        val tempTable = AcumeCacheContext.getTable(cube)
        rdd.registerTempTable(tempTable)
        sqlContext.sql(s"select *, $startTime as ts from $tempTable")
      }
      logger.info("Registering Temp Table " + tableName)
      finalRdd.registerTempTable(tableName)
      logger.info(s"Thick Query $updatedsql")
      val resultSchemaRDD = sqlContext.sql(updatedsql)
      new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD, MetaData(-1, timestamps.toList))
    }
  }
  
  protected def getTimestampsAndSql(sql: String) : (MutableList[Long], ((String, QueryOptionalParam), (List[Tuple], RequestType.RequestType)),  Long) = {
    
    val originalparsedsql = AcumeCacheContext.parseSql(sql)
    var correctsql = ISqlCorrector.getSQLCorrector(cacheConf).correctSQL(this, sql, (originalparsedsql._1.toList, originalparsedsql._2))
    
    var updatedsql = correctsql._1._1
    var updatedparsedsql = correctsql._2
  
    val l = updatedparsedsql._1(0)
    val cubeName = l.getCubeName
    val binsource = l.getBinsource
    val startTime = l.getStartTime
    val endTime = l.getEndTime
    val rt =  updatedparsedsql._2
    val queryOptionalParams = correctsql._1._2
    var timestamps : MutableList[Long] = MutableList[Long]()
    
    validateQuery(startTime, endTime, binsource)

    val level : Long = {
      if (queryOptionalParams.getTimeSeriesGranularity() != 0) {
          queryOptionalParams.getTimeSeriesGranularity()
      } else {
        cubeMap.get(CubeKey(cubeName, binsource)).getOrElse(throw new RuntimeException(s"Cube not found with name $cubeName and binsource $binsource")).baseGran.granularity
      }
    }
    
    if(rt == RequestType.Timeseries) {
      val startTimeCeiling = Utility.floorFromGranularity(startTime, level)
      val endTimeFloor = Utility.floorFromGranularity(endTime, level)
      timestamps = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
    }
    
    (timestamps, correctsql, level)
  }
  
}