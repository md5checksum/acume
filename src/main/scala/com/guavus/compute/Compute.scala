package com.guavus.compute

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Compute {
  
  val conf = new SparkConf().setAppName("Spark Pi")
  val spark = new SparkContext(conf)

  def main(args: Array[String]) = { 
     
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop
  }
  
  def sparkContext = spark 

}

