package com.guavus.acume.cache.workflow

import scala.collection.JavaConversions._
import scala.collection.immutable.SortedMap
import scala.collection.mutable.MutableList

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.core.CacheTimeSeriesLevelPolicy
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.sql.ISqlCorrector
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.utility.Tuple
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.RequestType.RequestType


/**
 * @author archit.thakur
 * 
 */
abstract class AcumeCacheContextTrait(val cacheSqlContext : SQLContext, val cacheConf: AcumeCacheConf) extends Serializable {
  
  @transient
  private [cache] var rrCacheLoader : RRCache = Class.forName(cacheConf.get(ConfConstants.rrloader)).getConstructors()(0).newInstance(this, cacheConf).asInstanceOf[RRCache]
  private [cache] val dataLoader : DataLoader = null
	
  lazy private [cache] val measureMap = AcumeCacheContextTraitUtil.measureMap
	lazy private [cache] val dimensionMap = AcumeCacheContextTraitUtil.dimensionMap
  lazy private [cache] val cubeMap = AcumeCacheContextTraitUtil.cubeMap.filter(cubeKey => cubeKey._2.dataSource.equalsIgnoreCase(cacheConf.getDataSourceName))
  lazy private [cache] val cubeList = AcumeCacheContextTraitUtil.cubeList.filter(cube => cube.dataSource.equalsIgnoreCase(cacheConf.getDataSourceName))
  private [cache] val cacheTimeseriesLevelPolicy = new CacheTimeSeriesLevelPolicy(SortedMap[Long, Int]()(implicitly[Ordering[Long]]) ++ Utility.getLevelPointMap(cacheConf.get(ConfConstants.acumecoretimeserieslevelmap)).map(x=> (x._1.level, x._2)))

  lazy val cacheBaseDirectory = cacheConf.get(ConfConstants.cacheBaseDirectory)

  // Cache the file system object. This inherits the limitation that Acume will work with one filesystem for one run.
  lazy val fs = (new Path(cacheBaseDirectory)).getFileSystem(cacheSqlContext.sparkContext.hadoopConfiguration)

  cacheSqlContext match {
    case hiveContext: HiveContext =>
    case hbaseContext : HBaseSQLContext =>
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  def acql(sql: String): AcumeCacheResponse = {
    AcumeCacheContextTraitUtil.setQuery(sql)
    try {
      if (cacheConf.getInt(ConfConstants.rrsize._1).get == 0) {
        executeQuery(sql)
      } else {
        rrCacheLoader.getRdd(sql)
      }
    } finally {
      AcumeCacheContextTraitUtil.unsetQuery()
    }
  }
  
  def isDimension(name: String) : Boolean =  {
    if(dimensionMap.contains(name)) {
      true 
    } else if(measureMap.contains(name)) {
      false
    } else {
        throw new RuntimeException("Field " + name + " nither in Dimension Map nor in Measure Map.")
    }
  }
  
  def getDefaultValue(fieldName: String) : Any = {
    if(isDimension(fieldName))
      dimensionMap.get(fieldName).get.getDefaultValue
    else
      measureMap.get(fieldName).get.getDefaultValue
  }
  
  lazy private [acume] val getCubeMap : Map[CubeKey, Cube] = cubeMap.toMap

  lazy private[acume] val getCubeList : List[Cube] = cubeList.toList

  def getCacheInstance[k, v](
      startTime: Long,
      endTime: Long,
      cube: CubeKey): AcumeCache[k, v] =
    throw new NotImplementedError("AcumeCacheContextTrait does not implement getCachePoints().")

  def getAggregateCacheInstance[k, v](
      startTime: Long,
      endTime: Long,
      cube: CubeKey): AcumeCache[k, v] =
    throw new NotImplementedError("AcumeCacheContextTrait does not implement getAggregateCachePoints().")

  
  private [acume] def getCube(cube: CubeKey) = getCubeMap.get(cube).getOrElse(throw new RuntimeException(s"cube $cube not found."))
  
  /* To be overrided by subclasses */
  private [acume] def executeQuery(sql : String) : AcumeCacheResponse
  
  private[acume] def getFieldsForCube(name: String, binsource: String): List[String] = {
    val cube = getCubeMap.getOrElse(CubeKey(name, binsource), throw new RuntimeException(s"Cube $name Not in AcumeCache knowledge."))
    cube.dimension.dimensionSet.map(_.getName) ++ cube.measure.measureSet.map(_.getName)
  }

  private[acume] def getAggregationFunction(stringname: String): String = {
    val measure = measureMap.getOrElse(stringname, throw new RuntimeException(s"Measure $stringname not in Acume knowledge."))
    measure.getAggregationFunction
  }

  private[acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube] = {
    val dimensionSet = scala.collection.mutable.Set[Dimension]()
    val measureSet = scala.collection.mutable.Set[Measure]()
    for(field <- lstfieldNames)
      if(isDimension(field))
        dimensionSet.+=(dimensionMap.get(field).get)
      else
        measureSet.+=(measureMap.get(field).get)
      val kCube = 
        for(cube <- getCubeList if(dimensionSet.toSet.subsetOf(cube.dimension.dimensionSet.toSet) && 
            measureSet.toSet.subsetOf(cube.measure.measureSet.toSet))) yield {
          cube
        }
    kCube.toList
  }
  
  private [acume] def validateQuery(startTime : Long, endTime : Long, binSource : String, dsName: String, cubeName: String) {
    if(startTime < BinAvailabilityPoller.getFirstBinPersistedTime(binSource) || endTime > BinAvailabilityPoller.getLastBinPersistedTime(binSource)){
      throw new RuntimeException("Cannot serve query. StartTime and endTime doesn't fall in the availability range.")
    }
    cubeMap.get(CubeKey(cubeName, binSource)).getOrElse(throw new RuntimeException(s"Cube not found with name $cubeName and binsource $binSource"))
  }
  
  protected def getTimestampsAndSql(sql: String) : (MutableList[Long], ((String, QueryOptionalParam), (List[Tuple], RequestType)),  Long) = {
    
    val originalparsedsql = AcumeCacheContextTraitUtil.parseSql(sql)
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
    
    validateQuery(startTime, endTime, binsource, cacheConf.getDataSourceName, cubeName)

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
