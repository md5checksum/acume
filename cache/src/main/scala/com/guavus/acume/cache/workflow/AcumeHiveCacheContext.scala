package com.guavus.acume.cache.workflow

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.common.QLType.QLType
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.utility.InsensitiveStringKeyHashMap
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.Dimension
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.utility.Utility

/**
 * @author kashish.jain
 *
 */
class AcumeHiveCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) extends AcumeCacheContextTrait { 
 
  private [cache] val dimensionMap = new InsensitiveStringKeyHashMap[Dimension]
  private [cache] val measureMap = new InsensitiveStringKeyHashMap[Measure]
  
  sqlContext match{
  case hiveContext: HiveContext =>
  case sqlContext: SQLContext => 
  case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  private [acume] def cacheSqlContext() : SQLContext = sqlContext
  
  Utility.unmarshalXML(conf.get(ConfConstants.businesscubexml), dimensionMap, measureMap)

  def cacheConf = conf
  rrCacheLoader = Class.forName(conf.get(ConfConstants.rrloader)).getConstructors()(0).newInstance(this, conf).asInstanceOf[RRCache]
 
  def isDimension(name: String) : Boolean =  {
    if(dimensionMap.contains(name)) {
      true 
    } else if(measureMap.contains(name)) {
      false
    } else {
        throw new RuntimeException("Field " + name + " nither in Dimension Map nor in Measure Map.")
    }
  }
  
  def getDefaultValue(fieldName: String) = {
	if(isDimension(fieldName))
      dimensionMap.get(fieldName).get.getDefaultValue
    else
      measureMap.get(fieldName).get.getDefaultValue
  }
  
  def acql(sql: String, qltype: String = null): AcumeCacheResponse = { 
     val ql : QLType.QLType = if(qltype == null)
      QLType.getQLType(conf.get(ConfConstants.qltype)) 
    else
      QLType.getQLType(qltype)
    
    validateQLType(ql)
    rrCacheLoader.getRdd((sql, ql))
  }
  
  def executeQuery(sql: String, qltype: QLType.QLType) = {
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet","true")
    val resultSchemaRDD = sqlContext.sql(sql)
    new AcumeCacheResponse(resultSchemaRDD, MetaData(-1, Nil))
  }
  
  private def validateQLType(qltype: QLType.QLType) = {
    if (!AcumeCacheContext.checkQLValidation(sqlContext, qltype))
      throw new RuntimeException(s"ql not supported with ${sqlContext}");
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

  private [cache] def checkQLValidation(sqlContext: SQLContext, qltype: QLType) = { 
    
    sqlContext match{
      case hiveContext: HiveContext =>
        qltype match{
          case QLType.hql | QLType.sql => true
          case rest => false
        }
      case sqlContext: SQLContext => 
        qltype match{
          case QLType.sql => true
          case rest => false
        }
    }
  }

}
