package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

//@RunWith(classOf[JUnitRunner])
class PartitionDistributorTest extends FlatSpec with Matchers {

   val conf = new SparkConf
   conf.set("spark.app.name", "Kashish")
   conf.set("spark.executor.instances", "4")
   val query = "select pop_id from telstra_database_9_pop_segment_mime_hourly_out___default_binsrc___60 where timestamp>=1401253200 and timestamp<=1401260400"
   println("SQL is " + query)
   println("num of executors " + 4)
   val sc = new SparkContext(conf)
   val hiveContext = new HiveContext(sc)
   val hiveRdd = hiveContext.sql(query)
   val numPartitions = hiveRdd.partitions.size
   println("NumPartitons = " + numPartitions)
   var partitonDistributorRDD = new PartitonDistributorRDD(sc, numPartitions)
   partitonDistributorRDD = partitonDistributorRDD.cache
  
//  override def beforeEach {
//     
//  }
  
  "PartitionDistributorTest" should " start without Exception " in {
    
    val zipped = partitonDistributorRDD.zipRdd(hiveRdd)
	println("Caching zipped Partition")
	zipped.cache
	
	println("Printing zippedRdd")
	zipped.collect.foreach(println)
	
	this.synchronized {
		this.wait()
	}
    
  }
   
  "PartitionDistributorTest2" should " start without Exception " in {
    
    val zipped = partitonDistributorRDD.zipRdd(hiveRdd)
	println("Caching zipped Partition")
	zipped.cache
	
	println("Printing zippedRdd")
	zipped.collect.foreach(println)
	
	this.synchronized {
		this.wait()
	}

  }
  
//  override def afterEach {
//    deleteOutFile("src/test/resources/CloneXmlTest/test-clone/out",false)
//  }

  
}
