package com.guavus.acume.cache.core

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import org.apache.spark.SparkConf

object AcumeCacheType extends Enumeration {

  val TreeCache = new AcumeCacheType("com.guavus.acume.cache.core.AcumeTreeCache", classOf[AcumeTreeCache])
  
  def getAcumeCacheType(name: String): AcumeCacheType = { 
    
    for(actualName <- AcumeCacheType.values){
      if(name equals actualName.name)
        return actualName
    }
    TreeCache
  }
  class AcumeCacheType(val name: String, val acumeCache: Class[_<:AcumeCache]) extends Val
  implicit def convertValue(v: Value): AcumeCacheType = v.asInstanceOf[AcumeCacheType]
  
  def main(args: Array[String]) { 
    
    val conf = new SparkConf
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "local")
    val sqlContext = new SQLContext(new SparkContext(conf))
    val conf123 = new AcumeCacheConf
    conf123.set(ConfConstants.businesscubexml, "/Users/archit.thakur/Documents/Code_Acume_Scala/cache/src/test/resources/cubdefinition.xml")
    conf123.set("acume.cache.core.variableretentionmap", "1h:720")
    conf123.set("acume.cache.baselayer.instainstanceid","0")
    conf123.set("acume.cache.baselayer.storagetype", "orc")
    conf123.set("acume.cache.core.timezone", "GMT")
    conf123.set("acume.cache.baselayer.instabase","instabase")
    conf123.set("acume.cache.baselayer.cubedefinitionxml", "cubexml")
    conf123.set("acume.cache.execute.qltype", "sql")
    val cntxt = new com.guavus.acume.cache.workflow.AcumeCacheContext(sqlContext, conf123)
    cntxt.acql("select * from searchEgressPeerCube_12345")
  }
}