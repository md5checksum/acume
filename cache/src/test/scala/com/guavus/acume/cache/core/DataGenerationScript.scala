package com.guavus.acume.cache.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.DoubleType
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.sql.hive.HiveContext
import scala.util.Random
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.types.BinaryType
import com.guavus.common.attval.IAttributeValueCreatorOperator
import java.nio.ByteBuffer
import com.guavus.common.attval.AttributeValueBufferOperatorFactory
import com.guavus.common.attval.IAttributeValueBufferOperator
import java.util.ArrayList
import com.guavus.common.attval.keyval.Configuration
import com.guavus.common.attval.keyval.Configuration

object DataGenerationScript {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    var i = 0
    val rdd = sc.parallelize((1l to 100000000l), 30).map(x => Row.fromSeq(Array(x))).cache
//    val rdd = sc.textFile("/data/seed_part1.csv").mapPartitions(helper.func1, true).repartition(10).cache

    var timestamp = args(0).toLong
    sqlContext.sql("use high_cardinality_24hr_avs")
    
  def deleteDirectory(dir : String) {
    val path = new Path(dir)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    fs.delete(path, true)
  }
    while (timestamp <= args(1).toLong) {
      val newrdd = rdd.sample(true, 0.5, 1)
      val y = newrdd.mapPartitions(helper.func(_, timestamp), true)

      val latestschema = StructType(List(StructField("MSISDN", LongType, true),
        StructField("FLOW_DOWN_BYTES", LongType, true),
        StructField("FLOW_UP_BYTES", LongType, true),
        StructField("AV_APPID_DOWNBYTES", BinaryType, true),
        StructField("AV_CONTENT_DOWNBYTES", BinaryType, true),
        StructField("ROAMING_COUNT", LongType, true),
        StructField("SESSION_COUNT", LongType, true),
        StructField("TOTAL_ROAMING_DURATION", LongType, true),
        StructField("TOTAL_SESSION_DURATION", LongType, true),
        StructField("roaming_FLOW_DOWN_BYTES", LongType, true),
        StructField("roaming_FLOW_UP_BYTES", LongType, true),
        StructField("exporttimestamp", LongType, true),
        StructField("timestamp", LongType, true)))
      val finalRdd = sqlContext.applySchema(y, latestschema)
      
      println(latestschema)
      try {
      sqlContext.sql("alter table f___edr_bin_source___3600_edr_flow_subs drop partition (exporttimestamp="+ timestamp +",timestamp=" + timestamp + ")")
      } catch {
        case e : Exception => 
      }
      sqlContext.sql("alter table f___edr_bin_source___3600_edr_flow_subs add partition (exporttimestamp="+ timestamp +",timestamp=" + timestamp + ")")
      deleteDirectory("/user/hive/warehouse/high_cardinality_24hr_avs.db/f___edr_bin_source___3600_edr_flow_subs/" + "exporttimestamp=" + timestamp+ "/timestamp=" + timestamp)
//      finalRdd.saveAsParquetFile("/data/" + "timestamp=" + timestamp)
      println("written")
      finalRdd.saveAsParquetFile("/user/hive/warehouse/high_cardinality_24hr_avs.db/f___edr_bin_source___3600_edr_flow_subs/" + "exporttimestamp=" + timestamp + "/timestamp=" + timestamp)
      timestamp += 3600l
    }

  }

  
  
}

object helper {
  
  var random : Random = null
  
  val numbers : Array[Int] = (for(i <- 1 to 100) yield {
    i
  }).toArray
  
   val creatorMap = scala.collection.mutable.Map[String, IAttributeValueCreatorOperator]();
  
  def func(y : Iterator[Row], timestamp : Long) = {
        if(random == null) {
          random = new Random()
        }
    	y.map(x=> {
    		val z = Row.fromSeq(x.map(z =>z) ++ Array[Any](random.nextLong, random.nextLong, getByteArray("E", 100), getByteArray("E", 100), random.nextLong, random.nextLong, random.nextLong, random.nextLong, random.nextLong, random.nextLong) ++ Array(timestamp, timestamp));
    		z
    	})
  }
  
  def func1(y : Iterator[Row]) = {
        if(random == null) {
          random = new Random()
        }
        var i =10000l 
    	y.map(x=> {
    		i+=1
    		Row.fromSeq(Array(i, x.getLong(0)))
    	})
  }
  
  var avsOperator: IAttributeValueBufferOperator = null
  def avsOp =
    {
      if (avsOperator == null) avsOperator = AttributeValueBufferOperatorFactory.create()
      avsOperator
    }
  
  
  def getByteArray(flavor: String, size : Int): Array[Byte] =
    {
      val measureMap = new java.util.HashMap[Integer, java.lang.Float]
      val newSize = random.nextInt(size)
      while(measureMap.size < newSize) {
        measureMap.put(numbers(random.nextInt(100)), random.nextFloat)
      }
      
      if (!creatorMap.contains(flavor)) {
        creatorMap.put(flavor, AttributeValueBufferOperatorFactory.create(flavor))
      }
      var avsCreator = creatorMap.get(flavor).get

      val bb = avsCreator.createAndInitialize(measureMap).array()
      bb
    }
}