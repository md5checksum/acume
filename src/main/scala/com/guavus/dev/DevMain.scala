package com.guavus.dev

import scala.math.random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Logging
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import com.guavus.mapred.common.collection._
import java.io.BufferedWriter
import java.io.FileWriter

object DevMain extends Logging {

  def main(args: Array[String]) = { 
    
    /*
     * Test Logging.
     
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    //export SPARK_LOG4J_CONF="/opt/spark/conf/spark-container-log4j.properties
    logError("env var = " + System.getenv("SPARK_LOG4J_CONF"))
    logTrace("driver logs - Trace")
    logDebug("driver logs - Debug")
    logInfo("driver logs - Info")
    logWarning("driver logs - Warning")
    logError("driver logs - Error")
    val count = spark.parallelize(1 to n, slices).map(mapper_func).reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop
    * 
    * 
    * 
    */
    
    Dev.readSeq("/Users/archit.thakur/Desktop/orc_file/X.MAPREDUCE.0.5");
  }
  
  def mapper_func(i: Int): Int = { 
    
    logError("env var = " + System.getenv("SPARK_LOG4J_CONF"))
    logTrace("executor logs - Trace")
    logDebug("executor logs - Debug")
    logInfo("executor logs - Info")
    logWarning("executor logs - Warning")
    logError("executor logs - Error")
    val x = random * 2 - 1
     val y = random * 2 - 1
     if (x*x + y*y < 1) 1 else 0
  }
  
  def readSeq(str: String) = {
    
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val path = new Path(str)
	val reader = new SequenceFile.Reader(fs, path, conf)
    
    val key = new DimensionSet
    val value = new MeasureSet
    
    val writer = new BufferedWriter(new FileWriter("/Users/archit.thakur/Documents/Code_Custom_SparkCache_Scala/del"))
    while(reader.next(key, value)){
      
      writer.write(key.toString + "\t" + value.toString + "\n" )
    }
    reader.close();
    writer.close();
  }
}