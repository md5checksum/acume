package com.guavus.acume.cache.core

import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.Cube
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.disk.utility.CubeUtil
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.utility.Utility
import org.apache.spark.sql.catalyst.types.StructType
import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.OrderedRDDFunctions
import org.apache.spark.SparkContext._
import com.guavus.acume.cache.workflow.TMOPartitioner

private[cache] class TmoCache(keyMap: Map[String, Any], acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube, cacheLevelPolicy: CacheLevelPolicyTrait, timeSeriesAggregationPolicy: CacheTimeSeriesLevelPolicy)
  extends AcumeFlatSchemaTreeCache(keyMap, acumeCacheContext, conf, cube, cacheLevelPolicy, timeSeriesAggregationPolicy) {
  
  override def processBackendData(rdd: SchemaRDD) : SchemaRDD = {
    val index = rdd.schema.fieldNames.indexWhere(_.equals(conf.get("acume.cache.index")))
    val rowRDD = rdd.map(row => (row.getLong(index), row)).repartitionAndSortWithinPartitions(TMOPartitioner.partitioner).values
    acumeCacheContext.sqlContext.applySchema(rowRDD, rdd.schema)  
  }
  
  override def mergeChildPoints(emptyRdd: SchemaRDD, rdds : Seq[SchemaRDD]) : SchemaRDD = {
    if(rdds.isEmpty)
      emptyRdd
    val queue = new Queue[RDD[Row]]
    queue.++=(rdds)
    
    val index = rdds(0).schema.fieldNames.indexWhere(_.equals(conf.get("acume.cache.index")))
    
    while(queue.size > 1) {
      val size = queue.size
      var i = 0
      while(i < size-1) {
        queue.enqueue(zipRDD(queue.dequeue,queue.dequeue, index))
        i = i+2
      }
    }
    val q = queue.dequeue
    sqlContext.applySchema(q, rdds(0).schema)
  }
  
  def zipRDD(rdd1: RDD[Row], rdd2: RDD[Row], index: Int) : RDD[Row] = {
    
    val unionRowRdd = rdd1.zipPartitions(rdd2)((itr1,itr2) => 
      {val arr = new ArrayBuffer[Row]
      
      var elem1: Row = null
      var elem2: Row = null
      
      if(itr1.hasNext) 
        elem1 = itr1.next
      
      if(itr2.hasNext)
        elem2 = itr2.next
        
      import scala.util.control.Breaks._
      breakable {
        while(true && elem1 != null && elem2 != null) {
          if(elem1.getLong(index) <= elem2.getLong(index)) {
        
            arr.+=(elem1)
            if(!itr1.hasNext) {
              arr.+=(elem2)
              break
            }
            elem1 = itr1.next
          } else if(elem1.getLong(index) > elem2.getLong(index)) {
            arr.+=(elem2)
            if(!itr2.hasNext) {
              arr.+=(elem1)
              break
            }
            elem2 = itr2.next
          }
        }
      }
      while(itr1.hasNext) {
        arr.+=(itr1.next)
      }
      while(itr2.hasNext) {
        arr.+=(itr2.next)
      }
      arr.iterator
    })
    
    unionRowRdd
  }
  
}