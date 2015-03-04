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
//  def main(args: Array[String]) {
//  
//   
//    
//    val conf = new SparkConf
//    conf.set("spark.master", "local")
//    conf.set("spark.app.name", "yarn-client")
//    conf.set("spark.sql.hive.convertMetastoreParquet", "true")
//    conf.set("spark.sql.parquet.binaryAsString", "true")
//    val sc = new SparkContext(conf)
//    val sqlContext = new HiveContext(sc)
//    val conf123 = new AcumeCacheConf
//    conf123.set(ConfConstants.businesscubexml, "src/test/resources/acumemural.xml")
//    conf123.set("acume.cache.core.variableretentionmap", "1h:53")
//    conf123.set("acume.cache.baselayer.instainstanceid","0")
//    conf123.set("acume.cache.baselayer.storagetype", "insta")
//    conf123.set("acume.cache.core.timezone", "GMT")
//    conf123.set("acume.cache.baselayer.instabase","/Users/archit.thakur/Downloads/parquetInstabase")
//    conf123.set("acume.cache.baselayer.cubedefinitionxml", "cubexml")
//    conf123.set("acume.cache.execute.qltype", "hql")
//    conf123.set("acume.cache.rrcache.loader", "com.guavus.acume.cache.workflow.RequestResponseCache")
//    conf123.set("acume.cache.core.rrcacheconcurrenylevel", "3")
//    conf123.set("acume.cache.core.rrcahcesize", "502")
////    conf123.set(ConfConstants.lastbinpersistedtime, "28800")
////    conf123.set(ConfConstants.firstbinpersistedtime, "3600")
//    conf123.set("acume.core.enableJDBCServer", "true")
//    conf123.set("acume.core.app.config", "com.guavus.acume.core.configuration.AcumeAppConfig")
//    conf123.set("acume.core.sql.query.engine", "acume")
//    conf123.set("acume.core.global.timezone", "GMT")
//    conf123.set(ConfConstants.backendDbName, "newdb1")
//    conf123.set(ConfConstants.cubedefinitionxml,"src/test/resources/muralinstacubedefinition.xml")
//    conf123.set(ConfConstants.acumecorebinsource, "__DEFAULT_BINSRC__")
//    conf123.set("acume.cache.default.cache.type", "AcumeStarSchemaTreeCache")
//    Utility.init(conf123)
//    
//    val rdd = sc.textFile("hdfs://nn1:9000/data/192.168.173.176.activejobs.txt").asInstanceOf[MappedRDD[String, String]]
//    val prefferedLocations = rdd.getPartitions.map(rdd.parent(0).asInstanceOf[HadoopRDD[LongWritable, Text]].getPreferredLocations(_))
//    println(prefferedLocations.size)
//    prefferedLocations.map(_.map(println))
//    val cntxt = new com.guavus.acume.cache.workflow.AcumeCacheContext(sqlContext, conf123)
////    cntxt.acql("select egressruleid from searchEgressPeerCube where ts >=1384750800 and ts <1384758000")
////    cntxt.acql("SELECT tx.sum_TTS_B AS TTS_B FROM (SELECT sum(TTS_B) AS sum_TTS_B FROM searchEgressPeerCube WHERE ts < 1384761600 AND ts >= 1384750800) tx")
////    cntxt.acql("SELECT (T1.sum_TTS_B/T2.totalsum) * T3.total1 AS percent_TTS_B, (T1.sum_TTS_B - T1.sum_Off_net_B)/T1.sum_Off_net_B * 100 AS growth_TTS_B, T1.FlowDirection AS FlowDirection, T1.EgressAS AS EgressAS FROM (SELECT sum(TTS_B) AS sum_TTS_B, sum(Off_net_B) AS sum_Off_net_B, FlowDirection, EgressAS FROM searchEgressPeerCube GROUP BY FlowDirection, EgressAS) T1 FULL JOIN (SELECT totalsum FROM (SELECT SUM(TTS_B) AS totalsum FROM searchEgressPeerCube) T1) T2 FULL JOIN (SELECT T1.FlowDirection, total1 FROM (SELECT FlowDirection, SUM(TTS_B) AS total1 FROM searchEgressPeerCube GROUP BY FlowDirection) T1) T3 ON T1.FlowDirection = T3.FlowDirection WHERE T1.sum_TTS_B > 5 and ts >=1384750800 and ts <1384754400")
////    conf123.set(ConfConstants.evictionpolicy, "com.guavus.acume.cache.eviction.VREvictionPolicy")
//    //cntxt.acql("select TTS_B from \"searchEgressPeerCube\" where ts >=3600 and ts <18000 and binsource = '60min'")
//    cntxt.acql("select HIT_COUNT from all_dev where ts >=1415620800 and ts <1415624400 and binsource = '__DEFAULT_BINSRC__'").schemaRDD.collect.map(x => println(x.mkString(",")))
//    cntxt.acql("select HIT_COUNT from searchEgressPeerCube where ts >=1415577600 and ts <1415664000 and binsource = '__DEFAULT_BINSRC__'").schemaRDD.collect.map(x => println(x.mkString(",")))
//    print("")
////    cntxt.acql("SELECT T1.ts AS ts, T1.sum_TTS_B AS TTS_B FROM (SELECT ts, sum(TTS_B) AS sum_TTS_B FROM searchEgressPeerCube WHERE ts < 36000 and  ts >= 3600 GROUP BY ts) T1")
////    cntxt.acql("SELECT tx.sum_TTS_B AS TTS_B FROM (SELECT sum(TTS_B) AS sum_TTS_B FROM searchEgressPeerCube WHERE ts < 1384761600 AND ts >= 1384750800) tx")
////    cntxt.acql("SELECT (T1.sum_TTS_B/T2.totalsum) * T3.total1 AS percent_TTS_B, (T1.sum_TTS_B - T1.sum_Off_net_B)/T1.sum_Off_net_B * 100 AS growth_TTS_B, T1.FlowDirection AS FlowDirection, T1.EgressAS AS EgressAS FROM (SELECT sum(TTS_B) AS sum_TTS_B, sum(Off_net_B) AS sum_Off_net_B, FlowDirection, EgressAS FROM searchEgressPeerCube GROUP BY FlowDirection, EgressAS) T1 FULL JOIN (SELECT totalsum FROM (SELECT SUM(TTS_B) AS totalsum FROM searchEgressPeerCube) T1) T2 FULL JOIN (SELECT T1.FlowDirection, total1 FROM (SELECT FlowDirection, SUM(TTS_B) AS total1 FROM searchEgressPeerCube GROUP BY FlowDirection) T1) T3 ON T1.FlowDirection = T3.FlowDirection WHERE T1.sum_TTS_B > 5 and ts >=1384750800 and ts <1384754400")
////    cntxt.acql("SELECT T1.sum_TTS_B AS percent_TTS_B, T1.FlowDirection AS FlowDirection, T1.EgressAS AS EgressAS FROM (SELECT sum(TTS_B) AS sum_TTS_B, sum(Off_net_B) AS sum_Off_net_B, FlowDirection, EgressAS FROM searchEgressPeerCube GROUP BY FlowDirection, EgressAS) T1 WHERE T1.sum_TTS_B > 5 and ts >=1384750800 and ts <1384754400")
////    cntxt.acql("SELECT (T1.sum_TTS_B/T2.totalsum) AS percent_TTS_B, T1.IngressAS AS IngressAS, T1.EgressAS AS EgressAS FROM (SELECT sum(TTS_B) AS sum_TTS_B, IngressAS, EgressAS, ts FROM searchEgressPeerCube GROUP BY IngressAS, EgressAS, ts) T1 FULL JOIN (SELECT totalsum, T1.ts AS ts FROM (SELECT SUM(TTS_B) AS totalsum, ts FROM searchEgressPeerCube GROUP BY ts) T1) T2 ON T1.ts = T2.ts WHERE T1.ts >= 1384750800 AND T1.ts < 1384754400 AND T1.sum_TTS_B >  5")
//    
//  
//
//}
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