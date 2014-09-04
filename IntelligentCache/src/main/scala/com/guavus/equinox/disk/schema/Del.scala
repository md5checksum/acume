package com.guavus.equinox.disk.schema

import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.spark.SparkConf
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import com.guavus.equinox.launch.SparkLauncher
import scala.math.random

object Del {

  def main(args: Array[String]){
    
    val sparkConf = new SparkConf
  sparkConf.set("spark.app.name", "yarn-client")
//  sparkConf.setMaster("yarn-cluster") 
  val sparkContextEquinox = new SparkContext(sparkConf) //context.sparkContextEquinox
//  FileWrapper.addLocalJar(sparkContextEquinox, "/data/archit/server_testing_scala/solution/WEB-INF/lib/")
  val sqlContext = new SQLContext(sparkContextEquinox)
//      val orcFile1 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressCustCubeDimension.orc")
//    val orcFile2 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressCustCubeMeasure.orc")
    import sqlContext._
    
    val x = List(1 to 10000)
    val x_ = sparkContext.parallelize(x, 2)
    val x$ = x_.map({ i =>
    if(random % 2 == 0)
      0 else 1
    }).reduce(_+_)
    
    println(x$)
    
//    val dimensionRdd = orcFile1.map(SparkLauncher.iSearchPRI_InteractionEgressDimension).registerAsTable("isearchIngressCustCubeDimension")
//    val measureRdd = orcFile2.map(SparkLauncher.iSearchPRI_InteractionEgressMeasure).registerAsTable("isearchIngressCustCubeMeasure")
//    
//    cacheTable("isearchIngressCustCubeDimension")
//    cacheTable("isearchIngressCustCubeMeasure")

//    println(sqlContext.sql("select * from isearchIngressCustCubeDimension"))
//    
    sparkContextEquinox.stop();
  }
}

