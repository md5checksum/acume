package org.apache.spark.scheduler.cluster

import scala.collection.Iterator
import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory
import org.slf4j.Logger

class PartitionDistributorRDD[T: ClassTag](@transient sc: SparkContext, numPartitions : Int) extends RDD[T](sc.emptyRDD) {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[PartitionDistributorRDD[T]])
  
  var partitions1 : Array[Partition] = _
  var partitionToExecutorMapping : TreeMap[Int, Int] = new TreeMap[Int, Int]() 
  var deadToAliveExecutorMapping : HashMap[Int, Int] = new HashMap[Int, Int]()
  val executorToHostMapping : TreeMap[Int, String] = getExecutorData
  val executorToHostMapSize = executorToHostMapping.keySet.size
  var maxExecutorId = executorToHostMapping.keySet.lastKey
  
  private def getNewAliveExecutorId(deadExecutorId : Int, split : Partition, executorData : TreeMap[Int, String]) : Int = {
   
   if(executorData.get(maxExecutorId + 1) != None) {
     maxExecutorId += 1
     return maxExecutorId
   }
   logger.info("No new executor added. Using existing executors")
   -1
  }
  
  private def updatePartitionToExecutorMapping(split : Partition, executorId : Int) {
    if(partitionToExecutorMapping.get(split.index) != None) {
      partitionToExecutorMapping = partitionToExecutorMapping.updated(split.index, executorId)
    }
  }

  override def getPartitions: Array[Partition] = {
    partitions1 = (for( i <- 0 until numPartitions) yield {
      new DistributorPartition(i)
    }).toArray
    partitions1
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val executorData = getExecutorData()
    
    var executorId = {
      if (partitionToExecutorMapping.get(split.index) == None) 
        split.index % executorToHostMapSize + 1
      else
        partitionToExecutorMapping.get(split.index).get
    }
    
    while(isExecutorDead(executorData, executorId)) {
      if(deadToAliveExecutorMapping.get(executorId) == None) {
        val aliveExecutorId = getNewAliveExecutorId(executorId, split, executorData)
        if(aliveExecutorId == -1) {
          val executorDataArray = executorData.keySet.toArray
          executorId = executorDataArray(split.index % executorData.size)
        } else {
          deadToAliveExecutorMapping.+=(executorId -> aliveExecutorId)
          executorId = aliveExecutorId
          updatePartitionToExecutorMapping(split, executorId)
        }
      } else {
        executorId = deadToAliveExecutorMapping.get(executorId).get
      }
    }

    Array(executorData.get(executorId).get)
  }
  
  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    Iterator.empty
  }
  
  def zipRdd[U: ClassTag](other: RDD[U]): RDD[U] = {
    zipPartitions(other, true) { (thisIter, otherIter) =>
      new Iterator[U] {
        def hasNext = otherIter.hasNext
        def next = otherIter.next
      }
    }
  }
  
  private def isExecutorDead(executorData : TreeMap[Int, String], executorId : Int) : Boolean = {
    (executorData.get(executorId) == None)
  }
  
  private def getExecutorData() = {
    val schedulerBackendField = sc.getClass().getDeclaredField("schedulerBackend")
    schedulerBackendField.setAccessible(true)
    val schedulerBackend = schedulerBackendField.get(sc)
    
    var executorData = new TreeMap[Int, String]()
    if(schedulerBackend.isInstanceOf[CoarseGrainedSchedulerBackend]) {
	  val executorDataMapField = classOf[CoarseGrainedSchedulerBackend].getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
	  executorDataMapField.setAccessible(true)
	    
	  while(executorData.isEmpty)
	    executorDataMapField.get(schedulerBackend).asInstanceOf[HashMap[String, ExecutorData]].map(x => executorData+=(x._1.toInt -> x._2.executorHost))

	  //printExecutorData(executorData)
    }
    executorData
  }
  
  private def printExecutorData(executorData : TreeMap[Int, String]) {
    executorData.foreach(x => logger.info(x._1 + " " + x._2))
  }
  
}

object PartitionDistributorRDD {
  
  //private var logger: Logger = LoggerFactory.getLogger(classOf[SampleRDD[]])
  
  private var instanceMap = new HashMap[Int, PartitionDistributorRDD[_]]()
  
  def getInstance(sparkContext : SparkContext, numPartitions : Int) : PartitionDistributorRDD[_] = {
    
    if(instanceMap.get(numPartitions) == None) {
      synchronized {
        if(instanceMap.get(numPartitions) == None) {
          instanceMap.put(numPartitions, new PartitionDistributorRDD(sparkContext, numPartitions))
        }
      }
    }
    instanceMap.get(numPartitions).get
  }
  
  def main(args: Array[String]) {
	val conf = new SparkConf
    conf.set("spark.app.name", "Kashish")
    conf.set("spark.executor.instances", args(0))
    
    println("SQL = " + args(1))
    println("Num of executors = " + args(0))
    
    val sc = new SparkContext(conf)
	val hiveContext = new HiveContext(sc)
	
	val hiveRdd = hiveContext.sql(args(1))
	val numPartitions = hiveRdd.rdd.partitions.size
	println("NumPartitions = " + numPartitions)
    
	var partitionDistributorRDD = new PartitionDistributorRDD(sc, numPartitions)
	partitionDistributorRDD = partitionDistributorRDD.cache

	val zipped = partitionDistributorRDD.zipRdd(hiveRdd.rdd)
	zipped.cache
	
	zipped.collect.foreach(println)
	
	this.synchronized {
		this.wait()
	}
  }
  
}
/**
 * A partition of an RDD.
 */
class DistributorPartition(indexi : Int) extends Partition {
  /**
   * Get the split's index within its parent RDD
   */
  def index: Int = indexi

  // A better default implementation of HashCode
  override def hashCode(): Int = index
}