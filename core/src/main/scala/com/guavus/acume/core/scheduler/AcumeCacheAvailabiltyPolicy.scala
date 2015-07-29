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

/**
 * 
 * @author archit.thakur
 */
class AcumeCacheAvailabiltyPolicy(acumeConf: AcumeConf, sqlContext: SQLContext) extends ICacheAvalabiltyUpdatePolicy(acumeConf, sqlContext) {
  
  override def getCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = super.getTrueCacheAvalabilityMap
    
  override def onBlockManagerRemoved(withMap: HashMap[String, HashMap[Long, Interval]] = HashMap[String, HashMap[Long, Interval]]()): Unit = {
    super.onBlockManagerRemoved(withMap)
  }
}   

class UnionizedCacheAvailabiltyPolicy(acumeConf: AcumeConf, sqlContext: SQLContext) extends ICacheAvalabiltyUpdatePolicy(acumeConf, sqlContext) {

  private val list = MutableList[HashMap[String, HashMap[Long, Interval]]]()
  private var unionizedMap = list.reduce(union(_, _))
  private var isUnionDirty = false
  
  override def getTrueCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = super.getTrueCacheAvalabilityMap
  
  override def getCacheAvalabilityMap: HashMap[String, HashMap[Long, Interval]] = {
    if(isUnionDirty) {
      unionizedMap = list.reduce(union(_, _))
      isUnionDirty = false  
    }
    unionizedMap
  }
    
  override def onBlockManagerRemoved(withMap: HashMap[String, HashMap[Long, Interval]] = HashMap[String, HashMap[Long, Interval]]()): Unit = {

    isUnionDirty = true
    if (!withMap.isEmpty)
      list.+=(withMap)
    list.+=(getTrueCacheAvalabilityMap)
    super.reset
  }
  
  override def preProcessSchemaRDD(unprocessed: SchemaRDD): SchemaRDD = {
    
    val executedPlan = unprocessed.queryExecution.executedPlan
    val id = if(executedPlan.isInstanceOf[InMemoryColumnarTableScan])
      executedPlan.asInstanceOf[InMemoryColumnarTableScan].relation._cachedColumnBuffers.id
    else throw new RuntimeException("UnionizedCacheAvailabiltyPolicy expects the schemardds of physical plan InMemoryColumnarTableScan")
      
    val _$processed = new SchemaRDD(sqlContext, Prune(customPartitionPruner(id), unprocessed.baseLogicalPlan))
    _$processed
  }
  
  private def customPartitionPruner(id: Int)(partitionId: Int) = {
    
    val blockId = RDDBlockId(id, partitionId)
    val iterable = sqlContext.sparkContext.ui.get.storageStatusListener.storageStatusList
    var flag = false
    for(i <- iterable){
      i._rddBlocks.get(id) match {
        case None => 
        case Some(idStatusMap) => idStatusMap.get(blockId) match {
          case None =>
          case Some(blockStatus) => if(blockStatus.isCached == true && blockStatus.storageLevel.useDisk == false && 
           blockStatus.storageLevel.useOffHeap == false && blockStatus.storageLevel.useMemory == true) 
            flag = true
          else 
            flag = false
        }
      }
    }
    flag
  }
  
  override def update(withMap: HashMap[String, HashMap[Long, Interval]] = HashMap[String, HashMap[Long, Interval]]()) = {
    
    super.update(withMap)
    list.clear()
    list.+=(getTrueCacheAvalabilityMap)
    isUnionDirty = true 
    
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
      val i3: Interval = new Interval
      i3.startTime = if (i1.startTime > i2.startTime) i2.startTime else i1.startTime
      i3.endTime = if (i1.endTime < i2.endTime) i2.endTime else i1.endTime
      i3
    }
  }

  private def union(map1: HashMap[String, HashMap[Long, Interval]], map2: HashMap[String, HashMap[Long, Interval]]): HashMap[String, HashMap[Long, Interval]] = {

    if (map1 == null || map1.isEmpty) map2
    else if (map2 == null || map2.isEmpty) map1
    else map1 ++ map2.map { case (k, v) => k -> (unionInternalMaps(v, map1.getOrElse(k, null))) }
  }
}
