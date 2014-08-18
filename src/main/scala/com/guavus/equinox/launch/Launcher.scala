package com.guavus.equinox.launch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Launcher {
	
  def main(args: Array[String]) = {
    
    val sparkConf = new SparkConf
    sparkConf.set("spark.app.name", "Equinox")
    
    val sparkContext = new SparkContext(sparkConf)
    loadCubeInformation()
    sparkContext.stop
  }
  
  def loadCubeInformation(hdfsPath: String) {
    
    
  }
}