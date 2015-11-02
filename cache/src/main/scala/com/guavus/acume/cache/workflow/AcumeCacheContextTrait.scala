package com.guavus.acume.cache.workflow

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.immutable.SortedMap
import scala.collection.mutable.MutableList
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.BaseCube
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
import com.guavus.qb.services.IQueryBuilderService
import net.sf.jsqlparser.statement.select.PlainSelect
import net.sf.jsqlparser.statement.select.Limit


/**
 * @author archit.thakur
 * 
 */
abstract class AcumeCacheContextTrait(val cacheSqlContext : SQLContext, val cacheConf: AcumeCacheConf) extends Serializable {
  
  @transient
  private [cache] var rrCacheLoader : RRCache = Class.forName(cacheConf.get(ConfConstants.rrloader)).getConstructors()(0).newInstance(this, cacheConf).asInstanceOf[RRCache]
  private [cache] val dataLoader : DataLoader = null
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeCacheContextTrait])
  
  lazy private [cache] val measureMap = AcumeCacheContextTraitUtil.measureMap
	lazy private [cache] val dimensionMap = AcumeCacheContextTraitUtil.dimensionMap
  lazy private [cache] val cubeMap = AcumeCacheContextTraitUtil.cubeMap.filter(cubeKey => cubeKey._2.dataSource.equalsIgnoreCase(cacheConf.getDataSourceName))
  lazy private [cache] val cubeList = AcumeCacheContextTraitUtil.cubeList.filter(cube => cube.dataSource.equalsIgnoreCase(cacheConf.getDataSourceName))
  private [cache] val cubeKeycacheTimeseriesLevelPolicyMap = scala.collection.mutable.Map[String, CacheTimeSeriesLevelPolicy]() ++ cubeMap.map(x => x._1.name + "_" + x._1.binsource -> new CacheTimeSeriesLevelPolicy(SortedMap[Long, Int]()(implicitly[Ordering[Long]]) ++ x._2.cacheTimeseriesLevelPolicyMap)).toMap
  
  lazy val cacheBaseDirectory = cacheConf.get(ConfConstants.cacheBaseDirectory)

  // Cache the file system object. This inherits the limitation that Acume will work with one filesystem for one run.
  lazy val fs = (new Path(cacheBaseDirectory)).getFileSystem(cacheSqlContext.sparkContext.hadoopConfiguration)

  cacheSqlContext match {
    case hiveContext: HiveContext =>
    case hbaseContext : HBaseSQLContext =>
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }

  def fireQuery(modifiedSql: String, requestDataType: RequestType.RequestType): AcumeCacheResponse = {

    // Execute Queries
    var count: Long = -1
    var rdd: RDD[Row] = null
    var limitValue: Int = -1
    var newDF: DataFrame = null
    var acumeCacheResponse = new AcumeCacheResponse(null, null, MetaData(-1, null))

    if (RequestType.Aggregate.equals(requestDataType) && !cacheConf.getDisableTotalForAggregateQueries(cacheConf.datasourceName)) {
      // This is aggregateQuery

      val limitAndQuery = getNoLimitQuery(modifiedSql)
      limitValue = limitAndQuery._1
      val noLimitQuery = limitAndQuery._2

      if (limitValue > 0) {
        // Fire count query and dont cache this
        acumeCacheResponse = executeQuery(noLimitQuery)
        rdd = acumeCacheResponse.rowRDD
        count = rdd.count
        newDF = cacheSqlContext.applySchema(rdd, acumeCacheResponse.schemaRDD.schema).limit(limitValue.toInt)
      } else {
        acumeCacheResponse = executeQuery(noLimitQuery)
        newDF = acumeCacheResponse.schemaRDD
      }
    } else {
      // This is a timeseries query or an aggregate query with count to be disabled
      // No need to remove the limit in timeseries query
      acumeCacheResponse = executeQuery(modifiedSql)
      newDF = acumeCacheResponse.schemaRDD
    }

    new AcumeCacheResponse(newDF, rdd, MetaData(count, acumeCacheResponse.metadata.timestamps))
  }

  def getNoLimitQuery(modifiedSql: String) = {
    //TODO Move this to queryBuilder

    var limitValue: Int = -1

    val stringBuilder = new StringBuilder(modifiedSql)
    val index = stringBuilder.lastIndexOf("LIMIT")

    if (index != -1) {
      var i = index + 6
      
      while (i < stringBuilder.length && stringBuilder.charAt(i).isDigit)
        i = i + 1

      limitValue = stringBuilder.substring(index + 6, i).toInt
      stringBuilder.delete(index, i)
    }

    val nonLimitQuery = stringBuilder.toString
    
    (limitValue, nonLimitQuery)
  }
  
  def acql(sql: String, queryBuilderService: Seq[IQueryBuilderService], requestDataType : RequestType.RequestType) : (AcumeCacheResponse, Array[Row]) = {
    var acumeResponse : AcumeCacheResponse = null
    var data : Array[Row] = null

    AcumeCacheContextTraitUtil.setQuery(sql)
    
    try {
      if (cacheConf.getInt(ConfConstants.rrsize._1).get == 0) {
        // RRcache is disabled
        acumeResponse = fireQuery(sql, requestDataType)
        data = acumeResponse.schemaRDD.collect
      } else {
        // RRcache is enabled
        acumeResponse = rrCacheLoader.getRdd(sql)
        val cachedRDD = acumeResponse.rowRDD.cache
        cachedRDD.checkpoint
        data = cachedRDD.collect
      }
    } finally {
      AcumeCacheContextTraitUtil.unsetQuery()
    }
    
    (acumeResponse, data)
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
  
  private [acume] def validateQuery(startTime : Long, endTime : Long, binSource : String, cubeName: String) {
    if(startTime < BinAvailabilityPoller.getFirstBinPersistedTime(binSource) || endTime > BinAvailabilityPoller.getLastBinPersistedTime(binSource)){
      throw new RuntimeException("Cannot serve query. StartTime and endTime doesn't fall in the availability range.")
    }
    cubeMap.get(CubeKey(cubeName, binSource)).getOrElse(throw new RuntimeException(s"Cube not found with name $cubeName and binsource $binSource"))
  }
  
  private [acume] def isThinClient : Boolean = {
    cacheConf.getOption(ConfConstants.useInsta) match {
      case Some(value) => return (!value.toBoolean)
      case None => return false
    }
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
    
    if(!isThinClient)
      validateQuery(startTime, endTime, binsource, cubeName)

    val level: Long = {
      if (queryOptionalParams.getTimeSeriesGranularity() != 0) {
        queryOptionalParams.getTimeSeriesGranularity()
      } else {
        val baseLevel = getCube(new CubeKey(cubeName, binsource)).baseGran.getGranularity
        if (rt == RequestType.Timeseries) {
          val cubetimeseriesMap = cubeKeycacheTimeseriesLevelPolicyMap.get(cubeName.toLowerCase + "_" + binsource)
          if (cubetimeseriesMap == None || cubetimeseriesMap.isEmpty) {
            Math.max(baseLevel, new CacheTimeSeriesLevelPolicy(SortedMap[Long, Int]()(implicitly[Ordering[Long]]) ++ Utility.getLevelPointMap(cacheConf.get(ConfConstants.acumecoretimeserieslevelmap)).map(x => (x._1.level, x._2)).toMap).getLevelToUse(startTime, endTime, BinAvailabilityPoller.getLastBinPersistedTime(binsource)))
          } else {
            Math.max(baseLevel, cubetimeseriesMap.get.getLevelToUse(startTime, endTime, BinAvailabilityPoller.getLastBinPersistedTime(binsource)))
          }
        } else {
          baseLevel
        }
      }
    }
    
    if(rt == RequestType.Timeseries) {
      val startTimeCeiling = Utility.floorFromGranularity(startTime, level)
      val endTimeFloor = Utility.floorFromGranularity(endTime, level)
      timestamps = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
    }
    
    (timestamps, correctsql, level)
  }

  // Firing on thin client
  protected def executeThinClientQuery(tsReplacedSql: String, timestamps : MutableList[Long])  : AcumeCacheResponse ={
      val resultSchemaRdd = cacheSqlContext.sql(tsReplacedSql)
      logger.info(s"Firing thin client Query $tsReplacedSql")
      new AcumeCacheResponse(resultSchemaRdd, resultSchemaRdd.rdd, new MetaData(-1, timestamps.toList))
  }
  
  // Firing on thick client
  protected def executeThickClientQuery(updatedsql: String, timestamps : MutableList[Long], cube : String, 
      binsource : String, rt : RequestType, startTime: Long, endTime : Long, level : Long, tableName: String) : AcumeCacheResponse = {

      val finalRdd = if (rt == RequestType.Timeseries) {
        val tables = for (timestamp <- timestamps) yield {
          
          val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null, null, null), timestamp, Utility.getNextTimeFromGranularity(timestamp, level, Utility.newCalendar), level)
          val tempTable = AcumeCacheContextTraitUtil.getTable(cube)
          rdd.registerTempTable(tempTable)
          val tempTable1 = AcumeCacheContextTraitUtil.getTable(cube)
          cacheSqlContext.sql(s"select *, $timestamp as ts from $tempTable").registerTempTable(tempTable1)
          tempTable1
        }

        val finalQuery = tables.map(x => s" select * from $x ").mkString(" union all ")
        cacheSqlContext.sql(finalQuery)

      } else {
        val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null, null, null), startTime, endTime, 0l)
        val tempTable = AcumeCacheContextTraitUtil.getTable(cube)
        rdd.registerTempTable(tempTable)
        cacheSqlContext.sql(s"select *, $startTime as ts from $tempTable")
      }
      
      logger.info(s"Registering Temp Table $tableName")
      finalRdd.registerTempTable(tableName)
      logger.info(s"Firing thick client Query $updatedsql")
      val resultSchemaRDD = cacheSqlContext.sql(updatedsql)
      new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD.rdd, MetaData(-1, timestamps.toList))
  }
  
}
