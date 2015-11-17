package com.guavus.acume.cache.core;

import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil
import com.guavus.acume.cache.disk.utility.CubeUtil
import org.apache.spark.sql.types.StructType
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.utility.Utility
import org.slf4j.LoggerFactory
import org.slf4j.Logger
import com.guavus.acume.cache.common.AcumeConstants
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import scala.collection.mutable.ArrayBuffer
import com.guavus.acume.cache.common.LevelTimestamp
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.Set
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import org.apache.hadoop.fs.Path

class PartitionedFlatSchemaTreeCache(keyMap: Map[String, Any], acumeCacheContext: AcumeCacheContext, 
                                         conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, 
                                         timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
     extends HashPartitionedFlatSchemaTreeCache(keyMap, acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[PartitionedFlatSchemaTreeCache].getSimpleName() + "-" + cube.getAbsoluteCubeName)
  
  val partitioningAttributes = cube.propertyMap.getOrElse(AcumeConstants.PARTITIONING_ATTRIBUTES, "").split(";")
    
  val partitioningAttributesValues: HashSet[Row] = new HashSet
  
  override def getDataFromBackend(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    import acumeCacheContext.cacheSqlContext._
    val cacheLevel = levelTimestamp.level
    val startTime = levelTimestamp.timestamp
    val endTime = Utility.getNextTimeFromGranularity(startTime, cacheLevel.localId, Utility.newCalendar)
    val diskloaded = diskUtility.loadData(keyMap, cube, startTime, endTime, cacheLevel.localId)

    sqlContext.setConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, "1")
    val partitionedColumnValues = diskloaded.select(partitioningAttributes.map(new Column(_)):_*).distinct
    val values = partitionedColumnValues.rdd.collect
    values.map(partitioningAttributesValues.+=(_))
    val partitioningAttr = values.map(_.toSeq.zip(partitioningAttributes).map(elem => elem._2.toString + "="+ elem._1.toString))
    
    val rdds: HashMap[String, SchemaRDD] = new HashMap
    for(i <- 0 to values.size-1) {
      val whereClause = partitioningAttr(i).mkString(" and ")
      val processedDiskLoaded = processBackendData(diskloaded.filter(whereClause), levelTimestamp)
      val path = "/" + partitioningAttr(i).mkString("/") + "/"
      rdds.put(path, processedDiskLoaded)
    }

    new PartitionedFlatSchemaCacheValue(acumeCacheContext, levelTimestamp, cube, cachePointToTable, rdds.toMap, processBackendData(diskloaded, levelTimestamp))
  }
    
  def processBackendData(rdd: SchemaRDD, levelTimestamp: LevelTimestamp) : SchemaRDD = {
    val tempTable = cube.getAbsoluteCubeName + "_" + System.currentTimeMillis
    rdd.registerTempTable(tempTable)
    sqlContext.setConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, numPartitions)
    AcumeCacheContextTraitUtil.setInstaTempTable(tempTable)
    val timestamp = levelTimestamp.timestamp
    val measureSet = (CubeUtil.getDimensionSet(cube) ++ CubeUtil.getMeasureSet(cube)).map(_.getName).mkString(",")
    sqlContext.sql(s"select $timestamp as ts, $measureSet from " + tempTable + str)
  }
  
  override def populateParentPointFromChildren(key : LevelTimestamp, acumeValRdds : Seq[(AcumeValue, SchemaRDD)], schema : StructType) : AcumeTreeCacheValue = {

    logger.info("Populating parent point from children for key " + key)
    val emptyRdd = Utility.getEmptySchemaRDD(sqlContext, schema)

    val _tableName = cube.getAbsoluteCubeName + key.level.toString + key.timestamp.toString

    val rdds = acumeValRdds.map(x => x._2)
    
    val values: Seq[Seq[String]] = partitioningAttributesValues.map(_.toSeq.zip(partitioningAttributes).map(elem => elem._2.toString + "="+ elem._1.toString)).toSeq

    sqlContext.setConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, numPartitions)
    val rdds3: HashMap[String, SchemaRDD] = new HashMap
    for(i <- 0 to values.size-1) {
      val whereClause = values(i).mkString(" and ")
      val rdds2 = rdds.map(_.filter(whereClause))
      val path = "/" + values(i).mkString("/") + "/"
      val value = mergeChildPoints(emptyRdd, rdds2)
      val (selectDimensions, selectMeasures, groupBy) = CubeUtil.getDimensionsAggregateMeasuresGroupBy(cube)

      val tempTable = _tableName + "Temp"
      value.registerTempTable(tempTable)
      AcumeCacheContextTraitUtil.setInstaTempTable(tempTable)
      val timestamp = key.timestamp
      var sortBy = ""
      if(secondaryIndex.size > 0) {
        sortBy = " sort by " + secondaryIndex
      }
    
      val parentRdd = acumeCacheContext.cacheSqlContext.sql(
        s"select $timestamp as ts " + (if(!selectDimensions.isEmpty) s", $selectDimensions " else "") + 
        (if(!selectMeasures.isEmpty) s", $selectMeasures" else "") + s" from $tempTable " + groupBy + sortBy)
        
      rdds3.put(path, parentRdd)
    }

    return new PartitionedFlatSchemaCacheValue(acumeCacheContext, key, cube, cachePointToTable, rdds3.toMap, rdds.reduce(_.zipAll(_)))
  }
  
  override def mergeChildPoints(emptyRdd : SchemaRDD, rdds : Seq[SchemaRDD]) : SchemaRDD = {
    if(rdds.isEmpty)
      return emptyRdd
    if(bucketingAttributes.isEmpty()) {
      return rdds.reduce(_.unionAll(_))
    }
    rdds.reduce(_.zipAll(_))
  }
  
  override def getRolledUpAcumeValue(levelTimestamp:LevelTimestamp, rdds: Seq[SchemaRDD]): AcumeTreeCacheValue = {
    
    val emptyRdd = Utility.getEmptySchemaRDD(sqlContext, rdds.head.schema)

    val values: Seq[Seq[String]] = partitioningAttributesValues.map(_.toSeq.zip(partitioningAttributes).map(elem => elem._2.toString + "="+ elem._1.toString)).toSeq

    val rdds3: HashMap[String, SchemaRDD] = new HashMap
    for(i <- 0 to values.size-1) {
      val whereClause = values(i).mkString(" and ")
      val rdds2 = rdds.map(_.filter(whereClause))
      val path = "/" + values(i).mkString("/") + "/"
      val value = mergeChildPoints(emptyRdd, rdds2)
        
      rdds3.put(path, value)
    }

    return new PartitionedFlatSchemaCacheValue(acumeCacheContext, levelTimestamp, cube, cachePointToTable, rdds3.toMap, rdds.reduce(_.zipAll(_)), false)
    
  }
  
  override def checkIfTableAlreadyExist(levelTimestamp: LevelTimestamp): AcumeTreeCacheValue = {
    val value = super.checkIfTableAlreadyExist(levelTimestamp)
    if(value != null) {
      sqlContext.setConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, "1")
      val partitionedColumnValues = value.getAcumeValue.measureSchemaRdd.select(partitioningAttributes.map(new Column(_)):_*).distinct
      val values = partitionedColumnValues.rdd.collect
      values.map(partitioningAttributesValues.+=(_))
    }
    value
  }

}