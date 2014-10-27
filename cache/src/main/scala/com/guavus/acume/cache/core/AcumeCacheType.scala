package com.guavus.acume.cache.core

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

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
    val sqlContext = new HiveContext(new SparkContext(conf))
    val conf123 = new AcumeCacheConf
    conf123.set(ConfConstants.businesscubexml, "src/test/resources/cubedefinition.xml")
    conf123.set("acume.cache.core.variableretentionmap", "1h:720")
    conf123.set("acume.cache.baselayer.instainstanceid","0")
    conf123.set("acume.cache.baselayer.storagetype", "text")
    conf123.set("acume.cache.core.timezone", "GMT")
    conf123.set("acume.cache.baselayer.instabase","/Users/archit.thakur/Downloads/instabase")
    conf123.set("acume.cache.baselayer.cubedefinitionxml", "cubexml")
    conf123.set("acume.cache.execute.qltype", "sql")
    conf123.set("acume.cache.rrcache.loader", "com.guavus.acume.cache.workflow.RequestResponseCache")
    conf123.set("acume.cache.core.rrcacheconcurrenylevel", "3")
    conf123.set("acume.cache.core.rrcahcesize", "502")
    conf123.set(ConfConstants.lastbinpersistedtime, "1384772400")
    val cntxt = new com.guavus.acume.cache.workflow.AcumeCacheContext(sqlContext, conf123)
    cntxt.acql("select TTS_B from searchEgressPeerCube where ts >=1384750800 and ts <1384754400")
  }
}