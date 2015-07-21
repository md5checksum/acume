
package com.guavus.acume.cache.core

import java.util.Arrays
import java.util.Random

import scala.Array.canBuildFrom
import scala.collection.immutable.SortedMap
import scala.collection.mutable.{ Map => MutableMap }
import scala.collection.mutable.MutableList

import org.apache.spark.AccumulatorParam
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.RemovalListener
import com.google.common.cache.RemovalNotification
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.DimensionTable
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.disk.utility.CubeUtil
import com.guavus.acume.cache.disk.utility.DataLoadedMetadata
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.utility.QueryOptionalParam
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.MetaData
import com.guavus.acume.cache.workflow.RequestType.Aggregate
import com.guavus.acume.cache.workflow.RequestType.RequestType
import com.guavus.acume.cache.workflow.RequestType.Timeseries
import com.google.common.cache.Cache
import com.guavus.acume.cache.common.LoadType

/**
 * @author archit.thakur
 *
 */
private[cache] class AcumeStarSchemaTreeCache(keyMap: Map[String, Any], acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeTreeCache(acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy) {

  @transient val sqlContext = acumeCacheContext.cacheSqlContext
  private val logger: Logger = LoggerFactory.getLogger(classOf[AcumeStarSchemaTreeCache])
  val dimensionTable: DimensionTable = DimensionTable("AcumeCacheGlobalDimensionTable" + cube.getAbsoluteCubeName, 0l)
  val diskUtility = DataLoader.getDataLoader(acumeCacheContext, conf, AcumeStarSchemaTreeCache.this)

  val cubeDimensionSet = CubeUtil.getDimensionSet(cube)
  val schema =
    cubeDimensionSet.map(field => {
      StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
    })
  val latestschema = StructType(schema.toList :+ StructField("id", LongType, true))
  Utility.getEmptySchemaRDD(acumeCacheContext.cacheSqlContext, latestschema).cache.registerTempTable(dimensionTable.tblnm)

  override def createTempTable(keyMap: List[Map[String, Any]], startTime: Long, endTime: Long, requestType: RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]) {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, false)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, false)
    }
  }

  val concurrencyLevel = conf.get(ConfConstants.rrcacheconcurrenylevel).toInt
  val acumetreecachesize = concurrencyLevel + concurrencyLevel * (cube.levelPolicyMap.map(_._2).reduce(_ + _))
  cachePointToTable = CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
    .maximumSize(acumetreecachesize).removalListener(new RemovalListener[LevelTimestamp, AcumeTreeCacheValue] {
      def onRemoval(notification: RemovalNotification[LevelTimestamp, AcumeTreeCacheValue]) {
        //        acumeCacheContext.sqlContext.uncacheTable(notification.getValue().measuretableName)
        //TODO check if data can be moved to disk
      }
    })
    .build(
      new CacheLoader[LevelTimestamp, AcumeTreeCacheValue]() {
        def load(key: LevelTimestamp): AcumeTreeCacheValue = {
          val output = checkIfTableAlreadyExist(key)
          if (output != null) {
            //        	  return new AcumeStarTreeCacheValue(new AcumeInMemoryValue(key, cube, ), output.measuretableName, output.measureschemardd)
            output
          } else {
            logger.info(s"Getting data from Insta for $key as it was never calculated")
          }
          //First check if point can be populated through children
          var schema: StructType = null
          try {
            var rdds = for (child <- cacheLevelPolicy.getChildrenIntervals(key.timestamp, key.level.localId)) yield {
              val _tableName = cube.cubeName + cacheLevelPolicy.getLowerLevel(key.level.localId) + child
              val outputRdd = sqlContext.table(_tableName)
              schema = outputRdd.schema
              outputRdd
            }
            if (schema != null) {
              return populateParentPointFromChildren(key, rdds, schema)
            }
          } catch {
            case _: Exception => logger.info(s"Getting data from Insta for $key as all children are not present")
          }
          if (key.loadType == LoadType.Insta)
            return getDataFromBackend(key);
          else
            throw new IllegalArgumentException("Couldnt populate parent point " + key + " from child points")
        }
      })

  private def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf("_"))

  override def createTempTableAndMetadata(keyMap: List[Map[String, Any]], startTime: Long, endTime: Long, requestType: RequestType, tableName: String, queryOptionalParam: Option[QueryOptionalParam]): MetaData = {
    requestType match {
      case Aggregate => createTableForAggregate(startTime, endTime, tableName, true)
      case Timeseries => createTableForTimeseries(startTime, endTime, tableName, queryOptionalParam, true)
    }
  }

  def populateParentPointFromChildren(key: LevelTimestamp, rdds: Seq[SchemaRDD], schema: StructType): AcumeTreeCacheValue = {
    val emptyRdd = Utility.getEmptySchemaRDD(sqlContext, schema).cache

    val _tableName = cube.getAbsoluteCubeName + key.level.toString + key.timestamp.toString

    val value = rdds.foldLeft(emptyRdd) { (result, current) =>
      current.unionAll(result)
    }

    import acumeCacheContext.sqlContext._
    val tempTable = _tableName + "tempUnion"

    value.registerTempTable(tempTable)
    //aggregate over measures after union
    val selectMeasures = CubeUtil.getMeasureSet(cube).map(x => x.getAggregationFunction + "(" + x.getName + ") as " + x.getName).mkString(",")
    val selectDimensions = CubeUtil.getDimensionSet(cube).map(_.getName).mkString(",")
    val parentRdd = acumeCacheContext.sqlContext.sql("select tupleid, " + key.timestamp + " as ts, " + selectMeasures + " from " + tempTable + " group by tupleid)")

    parentRdd.registerTempTable(_tableName)
    cacheTable(_tableName)
//    return new AcumeTreeCacheValue(dimensionTable.tblnm, _tableName, parentRdd)
    null
  }

  private def createTableForAggregate(startTime: Long, endTime: Long, tableName: String, isMetaData: Boolean): MetaData = {

    val duration = endTime - startTime
    val levelTimestampMap = cacheLevelPolicy.getRequiredIntervals(startTime, endTime)
    buildTableForIntervals(levelTimestampMap, tableName, isMetaData)
  }

  private def createTableForTimeseries(startTime: Long, endTime: Long, tableName: String, queryOptionalParam: Option[QueryOptionalParam], isMetaData: Boolean): MetaData = {

    val baseLevel = cube.baseGran.getGranularity
    val level =
      queryOptionalParam match {
        case Some(param) =>
          if (param.getTimeSeriesGranularity() != 0) {
            var level = Math.max(baseLevel, param.getTimeSeriesGranularity());
            val variableRetentionMap = getVariableRetentionMap
            if (!variableRetentionMap.contains(new Level(level))) {
              val headMap = variableRetentionMap.filterKeys(_.level < level);
              if (headMap.size == 0) {
                throw new IllegalArgumentException("Wrong granularity " + level + " passed in request which is not present in variableRetentionMap ");
              }
              level = headMap.lastKey.level
            }
            level
          } else
            Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, acumeCacheContext.getLastBinPersistedTime(cube.binsource)))
        case None =>
          Math.max(baseLevel, timeSeriesAggregationPolicy.getLevelToUse(startTime, endTime, acumeCacheContext.getLastBinPersistedTime(cube.binsource)))
      }

    val startTimeCeiling = cacheLevelPolicy.getCeilingToLevel(startTime, level)
    val endTimeFloor = cacheLevelPolicy.getFloorToLevel(endTime, level)
    val list = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
    if (!list.isEmpty) {
      val intervals: MutableMap[Long, MutableList[Long]] = MutableMap(level -> list)
      buildTableForIntervals(intervals, tableName, isMetaData)
    } else {
      Utility.getEmptySchemaRDD(acumeCacheContext.sqlContext, cube).registerTempTable(tableName)
      MetaData(-1, Nil)
    }
  }

  private def getVariableRetentionMap: SortedMap[Level, Int] = {
    val cubelocal = cube.levelPolicyMap
    SortedMap[Level, Int]() ++ cubelocal
  }

  override def getDataFromBackend(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    val _tableName = cube.getAbsoluteCubeName + levelTimestamp.level.toString + levelTimestamp.timestamp.toString
    import acumeCacheContext.sqlContext._
    val cacheLevel = levelTimestamp.level
    val diskloaded = loadData(cube, levelTimestamp, dimensionTable)

    val _$dataset = diskloaded._1
    val _$dimt = diskloaded._2
    val value = _$dataset
    value.registerTempTable(_tableName)
    cacheTable(_tableName)
    //    AcumeTreeCacheValue(_$dimt, _tableName, value)
    null //TODO Correct it
  }

  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable): Tuple2[SchemaRDD, String] = {
    val aggregatedTbl = "aggregatedMeasureDataInsta" + cube.getAbsoluteCubeName + levelTimestamp.level + "_" + levelTimestamp.timestamp
    diskUtility.loadData(keyMap, businessCube, levelTimestamp).registerTempTable(aggregatedTbl)

    this.synchronized {
      val endTime = Utility.getNextTimeFromGranularity(levelTimestamp.timestamp, levelTimestamp.level.localId, Utility.newCalendar)

      val metaData = diskUtility.getOrElseInsert(cube, new DataLoadedMetadata(Map[String, String](DataLoadedMetadata.dimensionSetStartTime -> "0", DataLoadedMetadata.dimensionSetEndTime -> "0")))
      val dimensionSetLoadedEndTime = metaData.get(DataLoadedMetadata.dimensionSetEndTime)
      if (dimensionSetLoadedEndTime.toLong >= endTime) {
        logger.info(s"Not loading dimension set from insta as dimensionSet is already loaded till $dimensionSetLoadedEndTime")
      } else {
        val dimensionSetRdd = diskUtility.loadDimensionSet(keyMap, businessCube, Utility.floorFromGranularity(dimensionSetLoadedEndTime.toLong, businessCube.baseGran.getGranularity), endTime)
        val fullRdd = AcumeStarSchemaTreeCache.generateId(dimensionSetRdd, dTableName, acumeCacheContext.cacheSqlContext, latestschema)
        val finalDimensionRdd = acumeCacheContext.sqlContext.table(dTableName.tblnm).unionAll(fullRdd)
        dTableName.Modify
        finalDimensionRdd.registerTempTable(dTableName.tblnm)
        metaData.put(DataLoadedMetadata.dimensionSetEndTime, endTime.toString)
      }
    }

    val sqlContext = acumeCacheContext.cacheSqlContext
    val dtnm = dTableName.tblnm
    val selectField = dtnm + ".id, " + CubeUtil.getCubeFields(businessCube).map(aggregatedTbl + "." + _).mkString(",")
    val onField = CubeUtil.getDimensionSet(businessCube).map(x => aggregatedTbl + "." + x.getName + "=" + dtnm + "." + x.getName).mkString(" AND ")
    val ar = sqlContext.sql(s"select $selectField from $aggregatedTbl left outer join $dtnm on $onField")
    //      val fullRdd = AcumeStarSchemaTreeCache.generateId(ar, dTableName, sqlContext)
    val joinedTbl = businessCube.getAbsoluteCubeName + levelTimestamp.level + "_" + levelTimestamp.timestamp
    ar.registerTempTable(joinedTbl)

    (correctMTable(businessCube, joinedTbl, levelTimestamp.timestamp),
      dTableName.tblnm)
  }

  def correctMTable(businessCube: Cube, joinedTbl: String, timestamp: Long) = {

    val measureSet = CubeUtil.getMeasureSet(businessCube).map(_.getName).mkString(",")
    sqlContext.sql(s"select id as tupleid, $timestamp as ts, $measureSet from $joinedTbl")
  }

  private def getUniqueRandomeNo: String = System.currentTimeMillis() + "" + Math.abs(new Random().nextInt)

  private def buildTableForIntervals(levelTimestampMap: MutableMap[Long, MutableList[Long]], tableName: String, isMetaData: Boolean): MetaData = {
    import acumeCacheContext.sqlContext._
    val timestamps: MutableList[Long] = MutableList[Long]()
    var finalSchema = null.asInstanceOf[StructType]
    val x = getCubeName(tableName)
    val levelTime = for (levelTsMapEntry <- levelTimestampMap) yield {
      val (level, ts) = levelTsMapEntry
      val cachelevel = CacheLevel.getCacheLevel(level)
      val timeIterated = for (item <- ts) yield {
        timestamps.+=(item)
        val levelTimestamp = LevelTimestamp(cachelevel, item)
        val acumeTreeCacheValue = cachePointToTable.get(levelTimestamp)
        notifyObserverList
        populateParent(levelTimestamp.level.localId, levelTimestamp.timestamp)
        val diskread = acumeTreeCacheValue.getAcumeValue.measureSchemaRdd
        finalSchema = diskread.schema
        val _$diskread = diskread
        _$diskread
      }
      timeIterated
    }
    if (!levelTime.isEmpty) {
      val schemarddlist = levelTime.flatten
      val dataloadedrdd = mergePathRdds(schemarddlist)
      val baseMeasureSetTable = cube.getAbsoluteCubeName + "MeasureSet" + getUniqueRandomeNo
      val joinDimMeasureTableName = baseMeasureSetTable + getUniqueRandomeNo
      dataloadedrdd.registerTempTable(baseMeasureSetTable)
      AcumeCacheUtility.dMJoin(acumeCacheContext.sqlContext, dimensionTable.tblnm, baseMeasureSetTable, joinDimMeasureTableName)
      val _$acumecache = table(joinDimMeasureTableName)
      if (logger.isTraceEnabled)
        _$acumecache.collect.map(x => logger.trace(x.toString))
      _$acumecache.registerTempTable(tableName)
    }
    val klist = timestamps.toList
    MetaData(-1, klist)
  }
}

object AcumeStarSchemaTreeCache {

  def generateId(drdd: SchemaRDD, dtable: DimensionTable, sqlContext: SQLContext, dimensionTableSchema: StructType): SchemaRDD = {

    //    val size = drdd.partitions.size
    //    drdd.coalesce(size/10+1)

    /**
     * formula = (k*n*10+index*10+max+j+1,k*n*10+index*10+10+max+j+1)
     */
    case class Vector(val data: Array[Long]) extends Serializable {}

    implicit object VectorAP extends AccumulatorParam[Vector] {
      def zero(v: Vector) = new Vector(new Array(v.data.size))
      def addInPlace(v1: Vector, v2: Vector) = {
        for (i <- 0 to v1.data.size - 1)
          v1.data(i) += v2.data(i)
        v1
      }
    }

    val numPartitions = drdd.rdd.partitions.size
    val accumulatorList = new Array[Long](numPartitions)
    Arrays.fill(accumulatorList, 0)
    val acc = sqlContext.sparkContext.accumulator(new Vector(accumulatorList))
    val lastMax = dtable.maxid
    //    def func(partionIndex : Int, itr : Iterator[Row]) = 
    import sqlContext.implicits._
    val rdd = drdd.rdd.mapPartitionsWithIndex((partionIndex, itr) => {
      var k, j = 0
      val temp = itr.map(x => {
        val arr = new Array[Any](x.size + 1)
        x.toSeq.copyToArray(arr)
        if (j >= 10) {
          j = 0
          k += 1
        }
        arr(x.size) = k * numPartitions * 10 + partionIndex * 10 + lastMax + 1 + j
        j += 1
        Row.fromSeq(arr)
      })
      val arr = new Array[Long](numPartitions)
      Arrays.fill(arr, 0)
      arr(partionIndex) = k * numPartitions * 10 + partionIndex * 10 + lastMax + 1 + j
      acc += new Vector(arr)
      temp
    })
//    import org.apache.spark.sql._
    val returnRdd = sqlContext.createDataFrame(rdd, dimensionTableSchema)
    print(returnRdd.schema)
    val tempDimensionTable = dtable.tblnm + "Temp"
    returnRdd.registerTempTable(tempDimensionTable)
    sqlContext.sql(s"cache table $tempDimensionTable")
    //    returnRdd.cache
    //    val newTuples = returnRdd.count
    //    println("Got " + newTuples + " new tuples for " + dtable.tblnm)
    dtable.maxid = acc.value.data.max
    returnRdd
  }

}
