package com.guavus.acume.cache.annotator

import org.apache.spark.rdd.RDD

object Annotator { 
  
  def aggregated_data() = { 
    
//    val cube = null.asInstanceOf[RDD[String]]
//    val other = null.asInstanceOf[RDD[String]]
//    cube.++(other)
//    
//    inputRDD
    
    
  }
  
//  private def cubeAnnotation
}
trait Annotator {

  def init(str: String*): Boolean
  def destroy(): Boolean
  def annotate()
  def getState(): AnnotatorState
}