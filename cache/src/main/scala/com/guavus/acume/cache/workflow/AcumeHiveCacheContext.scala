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
  
  Utility.unmarshalXML(conf.get(ConfConstants.businesscubexml), dimensionMap, measureMap)

  def cacheConf = conf
 
  private [acume] def getCubeList = throw new RuntimeException("Method not supported")
  
  def isDimension(name: String) : Boolean =  {
    if(dimensionMap.contains(name)) {
      true 
    } else if(measureMap.contains(name)) {
      false
    } else {
        throw new RuntimeException("Field " + name + " nither in Dimension Map nor in Measure Map.")
    }
  }
  
  private [acume] def getFieldsForCube(name: String) = {
    throw new RuntimeException("Method not supported")
  }
  
  def getDefaultValue(fieldName: String) = {
	if(isDimension(fieldName))
      dimensionMap.get(fieldName).get.getDefaultValue
    else
      measureMap.get(fieldName).get.getDefaultValue
  }
  
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]) = {
    throw new RuntimeException("Method not supported")
  }
  
  private [acume] def getAggregationFunction(stringname : String) : String = {
    throw new RuntimeException("Method not supported")
  }
  
  def acql(sql: String, qltype: String): AcumeCacheResponse = { 
    
    val ql = QLType.getQLType(qltype)
    if (!AcumeHiveCacheContext.checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"ql not supported with ${sqlContext}");
    executeQl(sql, ql)
  }
  
  def acql(sql: String): AcumeCacheResponse = { 
    
    val ql = AcumeHiveCacheContext.getQLType(conf)
    if (!AcumeHiveCacheContext.checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"ql not supported with ${sqlContext}");
    executeQl(sql, ql)
  }
  
  def executeQl(sql : String, ql : QLType.QLType) = {
    sqlContext.setConf("spark.sql.hive.convertMetastoreParquet","true")
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
  
  private def getQLType(conf: AcumeCacheConf) = QLType.getQLType(conf.get(ConfConstants.qltype)) 	

}
