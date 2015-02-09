package com.guavus.acume.cache.workflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.utility.Utility

/**
 * @author kashish.jain
 *
 */
class AcumeHiveCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) extends AcumeCacheContextTrait { 
 
  sqlContext match {
    case hiveContext: HiveContext =>
    case sqlContext: SQLContext => 
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  Utility.unmarshalXML(conf.get(ConfConstants.businesscubexml), dimensionMap, measureMap)
  
  private [acume] def cacheSqlContext() : SQLContext = sqlContext
  
  private [acume] def cacheConf = conf
  
  private [cache] def executeQuery(sql: String, qltype: QLType.QLType) = {
    val resultSchemaRDD = sqlContext.sql(sql)
    new AcumeCacheResponse(resultSchemaRDD, MetaData(-1, Nil))
  }
  
}

object AcumeHiveCacheContext{
  
  def main(args: Array[String]) { 
    
    val sqlContext = new SQLContext(new SparkContext)
    val conf = new AcumeCacheConf
    conf.set(ConfConstants.businesscubexml, "/Users/archit.thakur/Documents/Code_Acume_Scala/cache/src/test/resources/cubdefinition.xml")
    conf.set("acume.cache.core.variableretentionmap", "1h:720")
    conf.set("acume.cache.baselayer.instainstanceid","0")
    conf.set("acume.cache.baselayer.storagetype", "orc")
    conf.set("acume.cache.core.timezone", "GMT")
    conf.set("acume.cache.baselayer.instabase","instabase")
    conf.set("acume.cache.baselayer.cubedefinitionxml", "cubexml")
    conf.set("acume.cache.execute.qltype", "sql")
    conf.set("acume.core.enableJDBCServer", "true")
    conf.set("acume.core.app.config", "com.guavus.acume.core.configuration.AcumeAppConfig")
    conf.set("acume.core.sql.query.engine", "acume")
    
    val cntxt = new AcumeHiveCacheContext(sqlContext, conf)
    cntxt.acql("select * from searchEgressPeerCube_12345")
  }

  

}
