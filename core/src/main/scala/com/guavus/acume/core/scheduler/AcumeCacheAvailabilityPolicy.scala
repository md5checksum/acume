package com.guavus.acume.core.scheduler

import com.guavus.acume.core.AcumeConf
import com.guavus.acume.cache.core.Interval
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.core.AcumeContextTrait
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.columnar.InMemoryColumnarTableScan
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.sql.catalyst.plans.logical.Prune
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.cache.common.ConfConstants
import org.slf4j.LoggerFactory

/**
 *
 * @author archit.thakur
 */
class AcumeCacheAvailabilityPolicy(acumeConf: AcumeConf, sqlContext: SQLContext) extends ICacheAvalabilityUpdatePolicy(acumeConf, sqlContext) {

  override def getCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = {
    super.getCacheAvalabilityMap
  }
}

class UnionizedCacheAvailabilityPolicy(acumeConf: AcumeConf, sqlContext: SQLContext) extends ICacheAvalabilityUpdatePolicy(acumeConf, sqlContext) {

  val logger = LoggerFactory.getLogger(classOf[UnionizedCacheAvailabilityPolicy])
  private var map = HashMap[String, HashMap[Long, Interval]]()
  
  private def syncGet = {
    map = HashMap[String, HashMap[Long, Interval]]()
  }
  
  private def syncUnion = {
    val _$version = ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).getVersion
    val newmap = super.getLocalTrueCacheAvailabilityMap(_$version)
    if(!newmap.isEmpty)
      map = union(newmap, map)
  }
  
  override def getCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = {
    
    if(mode.equals("full")) {
      val _$version = ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).getVersion
      return super.getLocalTrueCacheAvailabilityMap(_$version)
    }
    else {
      val _$version = ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).getVersion
      val maps = union(map, super.getLocalTrueCacheAvailabilityMap(_$version))
      return maps
    }
  }

  override def onBlockManagerRemoved: Unit = {
    this.synchronized {
      mode = "partial"
      logger.info("Acume Mode switched to Partial.")
      syncUnion
    }
    super.onBlockManagerRemoved
  }

  override def preProcessSchemaRDD(unprocessed: SchemaRDD): SchemaRDD = {

    // Do not pre process if this is scheduler call.
    if (AcumeConf.acumeConf().getBoolean(ConfConstants.schedulerQuery, false) || mode.equals("full")) {
      return unprocessed
    }
    val executedPlan = unprocessed.queryExecution.executedPlan
    val id = if (executedPlan.isInstanceOf[InMemoryColumnarTableScan])
      executedPlan.asInstanceOf[InMemoryColumnarTableScan].relation._cachedColumnBuffers.id
    else return unprocessed

    val _$processed = new SchemaRDD(sqlContext, Prune(customPartitionPruner(id), unprocessed.baseLogicalPlan))
    _$processed
  }

  override def onBackwardCombinerCompleted(version: Int) {

    this.synchronized {
      mode = "full"
      logger.info("Acume Mode switched to Full.")
      if (ConfigFactory.getInstance.getBean(classOf[QueryRequestPrefetchTaskManager]).getVersion == version) {
        syncGet
      }
    }
  }

  private def customPartitionPruner(id: Int)(partitionId: Int) = {

    val blockId = RDDBlockId(id, partitionId)
    val iterable = sqlContext.sparkContext.ui.get.storageStatusListener.storageStatusList
    var flag = false
    for (i <- iterable) {
      i._rddBlocks.get(id) match {
        case None =>
        case Some(idStatusMap) => idStatusMap.get(blockId) match {
          case None =>
          case Some(blockStatus) => if (blockStatus.isCached == true && blockStatus.storageLevel.useDisk == false &&
            blockStatus.storageLevel.useOffHeap == false && blockStatus.storageLevel.useMemory == true)
            flag = true
          else
            flag = false
        }
      }
    }
    if(!flag) logger.info("partition "+id + "_" + partitionId + " pruned")
    flag
  }

  private def unionInternalMaps(map_1: HashMap[Long, Interval], map_2: HashMap[Long, Interval]): HashMap[Long, Interval] = {
    if (map_1 == null || map_1.isEmpty) map_2
    else if (map_2 == null || map_2.isEmpty) map_1
    else {
      map_1 ++ map_2.map {
        case (k, v) => k -> (unionInterval(v, map_1.getOrElse(k, null)))
      }
    }
  }

  private def unionInterval(i1: Interval, i2: Interval): Interval = {
    if (i1 == null) i2
    else if (i2 == null) i1
    else {
      val _$startTime = if (i1.startTime > i2.startTime) i2.startTime else i1.startTime
      val _$endTime = if (i1.endTime < i2.endTime) i2.endTime else i1.endTime
      val interval = new Interval(_$startTime, _$endTime)
      interval
    }
  }

  private def union(map1: HashMap[String, HashMap[Long, Interval]], map2: HashMap[String, HashMap[Long, Interval]]): HashMap[String, HashMap[Long, Interval]] = {

    if (map1 == null || map1.isEmpty) map2
    else if (map2 == null || map2.isEmpty) map1
    else map1 ++ map2.map { case (k, v) => k -> (unionInternalMaps(v, map1.getOrElse(k, null))) }
  }
}
