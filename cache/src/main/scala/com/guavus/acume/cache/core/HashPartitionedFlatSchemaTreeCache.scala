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
import org.apache.spark.sql.DataFrame

class HashPartitionedFlatSchemaTreeCache(keyMap: Map[String, Any], acumeCacheContext: AcumeCacheContext, 
                                         conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, 
                                         timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
     extends AcumeFlatSchemaTreeCache(keyMap, acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy) {
  
  private val logger: Logger = LoggerFactory.getLogger(classOf[HashPartitionedFlatSchemaTreeCache].getSimpleName() + "-" + cube.getAbsoluteCubeName)
  
  val bucketingAttributes = cube.propertyMap.getOrElse(AcumeConstants.BUCKETING_ATTRIBUTES, "").split(";").mkString(",")
  val secondaryIndex = cube.propertyMap.getOrElse(AcumeConstants.SECONDARY_INDEX, "").split(";").mkString(",")
  val numPartitions = cube.propertyMap.getOrElse(AcumeConstants.NUM_PARTITIONS, throw new IllegalArgumentException(AcumeConstants.NUM_PARTITIONS + " is missing"))
  
  def getPostProcessingString() : String = {

    val str = new StringBuilder
    if(bucketingAttributes.size > 0) {
      str.append(AcumeConstants.DISTRIBUTE_BY)
      str.append(bucketingAttributes)
    }
    if(secondaryIndex.size > 0) {
      str.append(AcumeConstants.SORT_BY)
      str.append(secondaryIndex)
    }
    str.toString
  } 
  
  val str = getPostProcessingString
    
  override def processBackendData(rdd: SchemaRDD) : SchemaRDD = {
    val tempTable = cube.getAbsoluteCubeName + "_" + System.currentTimeMillis
    rdd.registerTempTable(tempTable)
    sqlContext.setConf(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, numPartitions)
    AcumeCacheContextTraitUtil.setInstaTempTable(tempTable)
    sqlContext.sql(s"select * from " + tempTable + str)
  }
  
  override def populateParentPointFromChildren(key : LevelTimestamp, acumeValRdds : Seq[(AcumeValue, SchemaRDD)], schema : StructType) : AcumeTreeCacheValue = {

    logger.info("Populating parent point from children for key " + key)
    val emptyRdd = Utility.getEmptySchemaRDD(sqlContext, schema)

    val _tableName = cube.getAbsoluteCubeName + key.level.toString + key.timestamp.toString

    val value = mergeChildPoints(emptyRdd, acumeValRdds.map(x => x._2))
    
    //aggregate over measures after merging child points
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
    return new AcumeFlatSchemaCacheValue(new AcumeInMemoryValue(key, cube, parentRdd, cachePointToTable, acumeValRdds), acumeCacheContext)
  }
  
  override def mergeChildPoints(emptyRdd : SchemaRDD, rdds : Seq[SchemaRDD]) : SchemaRDD = {
    if(rdds.isEmpty)
      return emptyRdd
    zipChildPoints(rdds)
  }
  
  override def zipChildPoints(rdds : Seq[SchemaRDD]): SchemaRDD = {
    rdds.reduce(_.zipAll(_))
  }
  
  override def mergePathRdds(rdds : Iterable[DataFrame]) = {
    Utility.withDummyCallSite(acumeCacheContext.cacheSqlContext.sparkContext) {
        rdds.reduce(_.zipAll(_))
    }
  }
}