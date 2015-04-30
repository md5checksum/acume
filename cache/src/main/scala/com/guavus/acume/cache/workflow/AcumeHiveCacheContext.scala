package com.guavus.acume.cache.workflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.sql.ISqlCorrector
import scala.collection.mutable.ArrayBuffer
import com.guavus.acume.cache.common.Cube
import scala.collection.JavaConversions._
import com.guavus.acume.cache.disk.utility.DataLoader
import java.util.concurrent.ConcurrentHashMap
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.disk.utility.InstaDataLoaderThinAcume
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.core.FixedLevelPolicy

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
  Utility.init(conf)
  
  val dataLoader: DataLoader = new InstaDataLoaderThinAcume(this, conf, null)
  override private [cache] val dataloadermap = new ConcurrentHashMap[String, DataLoader]
  
  Utility.unmarshalXML(conf.get(ConfConstants.businesscubexml), dimensionMap, measureMap)
  
  private [acume] def cacheSqlContext() : SQLContext = sqlContext
  
  private [acume] def cacheConf = conf
  
  private [acume] def getCubeMap = throw new RuntimeException("Operation not supported")
  
  private [acume] def executeQuery(sql: String, qltype: QLType.QLType) = {
    if(!cacheConf.getBoolean(ConfConstants.useInsta, false)) {
      val resultSchemaRdd = sqlContext.sql(sql)
      new AcumeCacheResponse(resultSchemaRdd, resultSchemaRdd, new MetaData(-1, Nil))
    } else {
    val originalparsedsql = AcumeCacheContext.parseSql(sql)
    
    println("AcumeRequest obtained " + sql)
    var correctsql = ISqlCorrector.getSQLCorrector(conf).correctSQL(this, sql, (originalparsedsql._1.toList, originalparsedsql._2))
    var updatedsql = correctsql._1._1
    val queryOptionalParams = correctsql._1._2
    var updatedparsedsql = correctsql._2
    
    val rt = updatedparsedsql._2
      
    var i = ""
      var timestamps = scala.collection.mutable.MutableList[Long]() 
    val list = for(l <- updatedparsedsql._1) yield {
      val cube = l.getCubeName
      val binsource = l.getBinsource
      val startTime = l.getStartTime
      val endTime = l.getEndTime
    
      val key_binsource = 
        if(binsource != null)
          binsource
      else
        conf.get(ConfConstants.acumecorebinsource)

      i = AcumeCacheContext.getTable(cube)
      updatedsql = updatedsql.replaceAll(s"$cube", s"$i")
      val finalRdd = if(rt == RequestType.Timeseries) {
        val level = queryOptionalParams.getTimeSeriesGranularity
    	  val startTimeCeiling = Utility.floorFromGranularity(startTime, level)
    	  val endTimeFloor = Utility.floorFromGranularity(endTime, level)
           timestamps = Utility.getAllIntervals(startTimeCeiling, endTimeFloor, level)
          val tables = for(timestamp <- timestamps) yield {
        	  val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null), timestamp, Utility.getNextTimeFromGranularity(timestamp, level, Utility.newCalendar))
        	  val tempTable = AcumeCacheContext.getTable(cube)
        	  rdd.registerTempTable(tempTable)
        	  val tempTable1 = AcumeCacheContext.getTable(cube)
        	  sqlContext.sql(s"select *, $timestamp as ts from $tempTable").registerTempTable(tempTable1)
        	  tempTable1
          }
        val finalQuery = tables.map(x => s" select * from $x ").mkString(" union all ")
          sqlContext.sql(finalQuery)
      } else {
    	  val rdd = dataLoader.loadData(Map[String, Any](), new BaseCube(cube, binsource, null, null, null), startTime, endTime)
    	  val tempTable = AcumeCacheContext.getTable(cube)
        	  rdd.registerTempTable(tempTable)
        	  sqlContext.sql(s"select *, $startTime as ts from $tempTable")
      }
      print("Registering Temp Table " + i)
      finalRdd.registerTempTable(i)
    }
    print("Thin Query " + updatedsql)
    val resultSchemaRDD = sqlContext.sql(updatedsql)
    new AcumeCacheResponse(resultSchemaRDD, resultSchemaRDD, MetaData(-1, timestamps.toList))
  }
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