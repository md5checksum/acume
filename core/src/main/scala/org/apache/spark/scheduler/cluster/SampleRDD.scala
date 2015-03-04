package org.apache.spark.scheduler.cluster

import scala.Array.fallbackCanBuildFrom
import scala.collection.Iterator
import scala.reflect.ClassTag
import org.apache.spark.Partition
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.MappedRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.HadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

class SampleRDD[T: ClassTag](@transient sc: SparkContext, numPartitions : Int) extends RDD[T](sc.emptyRDD) {

  
  var partitions1 : Array[Partition] = _
  override def getPartitions: Array[Partition] = {
    partitions1 = (for( i <- 0 until numPartitions) yield {
      new SamplePartition(i)
    }).toArray
    partitions1
  }
//
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val schedulerBackendField = sc.getClass().getDeclaredField("schedulerBackend")
    schedulerBackendField.setAccessible(true)
    val schedulerBackend = schedulerBackendField.get(sc)
    println(schedulerBackend.getClass())
    val executorDataMapField = classOf[CoarseGrainedSchedulerBackend].getDeclaredField("org$apache$spark$scheduler$cluster$CoarseGrainedSchedulerBackend$$executorDataMap")
    executorDataMapField.setAccessible(true)
    val executorData = executorDataMapField.get(schedulerBackend).asInstanceOf[scala.collection.mutable.HashMap[String, ExecutorData]].valuesIterator.map(x => x.executorHost).toArray
    Array(executorData(split.index%executorData.size))
  }

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    Iterator.empty
  }
}


object SampleRDD {
  
  def main(args: Array[String]) {
	val conf = new SparkConf
    conf.set("spark.app.name", "yarn-client")
    val sc = new SparkContext(conf)
    val rdd = new SampleRDD(sc, 4)
	rdd.getPartitions.map(rdd.getPreferredLocations(_)).map(_.map(println))
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