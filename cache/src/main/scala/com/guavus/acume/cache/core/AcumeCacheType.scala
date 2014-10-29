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
    conf123.set("acume.cache.execute.qltype", "hql")
    conf123.set("acume.cache.rrcache.loader", "com.guavus.acume.cache.workflow.RequestResponseCache")
    conf123.set("acume.cache.core.rrcacheconcurrenylevel", "3")
    conf123.set("acume.cache.core.rrcahcesize", "502")
    conf123.set(ConfConstants.lastbinpersistedtime, "1384772400")
    val cntxt = new com.guavus.acume.cache.workflow.AcumeCacheContext(sqlContext, conf123)
    cntxt.acql("select egressruleid from searchEgressPeerCube where ts >=1384750800 and ts <1384754400")
//    cntxt.acql("SELECT (T1.sum_TTS_B/T2.totalsum) * T3.total1 AS percent_TTS_B, (T1.sum_TTS_B - T1.sum_Off_net_B)/T1.sum_Off_net_B * 100 AS growth_TTS_B, T1.FlowDirection AS FlowDirection, T1.EgressAS AS EgressAS FROM (SELECT sum(TTS_B) AS sum_TTS_B, sum(Off_net_B) AS sum_Off_net_B, FlowDirection, EgressAS FROM searchEgressPeerCube GROUP BY FlowDirection, EgressAS) T1 FULL JOIN (SELECT totalsum FROM (SELECT SUM(TTS_B) AS totalsum FROM searchEgressPeerCube) T1) T2 FULL JOIN (SELECT T1.FlowDirection, total1 FROM (SELECT FlowDirection, SUM(TTS_B) AS total1 FROM searchEgressPeerCube GROUP BY FlowDirection) T1) T3 ON T1.FlowDirection = T3.FlowDirection WHERE T1.sum_TTS_B > 5 and ts >=1384750800 and ts <1384754400")
  }
}

