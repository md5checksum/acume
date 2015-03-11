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

class SampleRDD[T: ClassTag](@transient sc: SparkContext, numPartitions : Int) extends RDD[T](sc.emptyRDD) {
  
  var partitions1 : Array[Partition] = _
  
  val executorMapping  : TreeMap[String, String] = getExecutorData
  
  override def getPartitions: Array[Partition] = {
    partitions1 = (for( i <- 0 until numPartitions) yield {
      new SamplePartition(i)
    }).toArray
    partitions1
  }
  

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val executorData = getExecutorData()
    if(executorData.isEmpty)
     return Seq()
      
    val executorId = (split.index % executorMapping.keySet.size + 1).toString
    println("Executor id = " + executorId)
    
    if(isExecutorDead(executorData, executorId)){
      var executorDataArray = executorData.values.toArray
      println("partition " + split.index + " is dead. Executor = " + executorDataArray(split.index % executorData.size))
      Array(executorDataArray(split.index % executorData.size))
    } else {
      println("partition " + split.index + " is alive. Executor = " + executorMapping.get(executorId).get)
      Array(executorMapping.get(executorId).get)
    }
  }
  
  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    Iterator.empty
  }
  
  private def zipRdd[U: ClassTag](other: RDD[U]): RDD[U] = {
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
      new Iterator[U] {
        def hasNext = otherIter.hasNext
        def next = otherIter.next
      }
    }
  }
  
  private def isExecutorDead(executorData : TreeMap[String, String], executorId : String) : Boolean = {
    if(executorData.keySet.contains(executorId))
      return false
    true
  }
  
//  private def getExecutorData2() = {
//    val schedulerBackendField = sc.getClass().getDeclaredField("schedulerBackend")
//    schedulerBackendField.setAccessible(true)
//    val schedulerBackend = schedulerBackendField.get(sc)
//    
//    var executorData = new TreeMap[String, String]()
//    println("=========== GetExecutorData called with scheduler class = " + schedulerBackend.getClass())
//    if(schedulerBackend.isInstanceOf[CoarseGrainedSchedulerBackend]) {
//	  val executorDataMapField = classOf[CoarseGrainedSchedulerBackend].getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
//	  executorDataMapField.setAccessible(true)
//	    
//	  while(executorData.isEmpty)
//	    executorDataMapField.get(schedulerBackend).asInstanceOf[HashMap[String, ExecutorData]].map(x => executorData+=(x._1 -> x._2.executorHost))
//	    printExecutorData(executorData)
//    }
//    executorData
//  }
  
  private def getExecutorData() = {
    val schedulerBackendField = sc.getClass().getDeclaredField("schedulerBackend")
    schedulerBackendField.setAccessible(true)
    val schedulerBackend = schedulerBackendField.get(sc)
    
    var executorData = new TreeMap[String, String]()
    println("=========== GetExecutorData called with scheduler class = " + schedulerBackend.getClass())
    if(schedulerBackend.isInstanceOf[CoarseGrainedSchedulerBackend]) {
	  val executorDataMapField = classOf[CoarseGrainedSchedulerBackend].getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
	  executorDataMapField.setAccessible(true)
	    
	  while(executorData.isEmpty)
	    executorDataMapField.get(schedulerBackend).asInstanceOf[HashMap[String, ExecutorData]].map(x => executorData+=(x._1 -> x._2.executorHost))
	    executorData.-=("1")
	    printExecutorData(executorData)
    }
    executorData
  }
  
  private def printExecutorData(executorData : TreeMap[String, String]) {
    println("======= printing executorData")
    executorData.foreach(x => println(x._1 + " " + x._2))
  }
  
}


object SampleRDD {
  
  def main(args: Array[String]) {
	val conf = new SparkConf
    conf.set("spark.app.name", "yarn-client")
    conf.set("spark.executor.instances", args(0))
    
    println(" =============== SQL is " + args(1))
    println(" =============== num of executors " + args(0))
    
    val sc = new SparkContext(conf)
	val hiveContext = new HiveContext(sc)
	
	val hiveRdd = hiveContext.sql(args(1))
	val numPartitions = hiveRdd.partitions.size
	println(" ======== NumPartitons = " + numPartitions)
    
	var sampleRdd = new SampleRDD(sc, numPartitions)
	sampleRdd = sampleRdd.cache

	val zipped = sampleRdd.zipRdd(hiveRdd)

	println(" ========== printing zippedRdd")
	zipped.collect.foreach(println)

  }
  
}
/**
 * A partition of an RDD.
 */
class SamplePartition(indexi : Int) extends Partition {
  /**
   * Get the split's index within its parent RDD
   */
  def index: Int = indexi

  // A better default implementation of HashCode
  override def hashCode(): Int = index
}