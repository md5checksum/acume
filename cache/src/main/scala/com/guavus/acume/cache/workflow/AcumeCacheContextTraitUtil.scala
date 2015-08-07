package com.guavus.acume.cache.workflow

import scala.collection.mutable.ArrayBuffer
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import com.guavus.acume.cache.common.AcumeConstants
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.utility.InsensitiveStringKeyHashMap
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.AcumeCacheConf

/**
 * @author kashish.jain
 */
object AcumeCacheContextTraitUtil {
  
  val dimensionMap = new InsensitiveStringKeyHashMap[Dimension]
  val measureMap = new InsensitiveStringKeyHashMap[Measure]
  val cubeMap = new HashMap[CubeKey, Cube]
  val cubeList = MutableList[Cube]()
  private val cacheConf = new AcumeCacheConf
  private val inheritablePoolThreadLocal = new InheritableThreadLocal[HashMap[String, Any]]()
  
  
  /*
   * Initializing and reading acume's cubedefiniton.xml
   * 
   */
  Utility.init(cacheConf)
  Utility.loadXML(cacheConf, dimensionMap, measureMap, cubeMap, cubeList)

  
  /*
   * Setting pool level threadlocal params
   */
  def poolThreadLocal: InheritableThreadLocal[HashMap[String, Any]] = inheritablePoolThreadLocal
  

  /*
   * Setting threadLocal params for query execution
   */
  private val threadLocal = new ThreadLocal[HashMap[String, Any]]() { 
    override protected def initialValue() : HashMap[String, Any] = {
      new HashMap[String, Any]()
    }
  }
  
  def setQuery(query : String) {
    threadLocal.get.put("query", query)
  }
  
  def getQuery(): String = {
    return threadLocal.get.getOrElse("query", null).asInstanceOf[String]
  }
  
  def unsetQuery() {
    threadLocal.get.put("query", null)
  }
  
  def addAcumeTreeCacheValue(acumeTreeCacheValue : AcumeTreeCacheValue) {
    val list = threadLocal.get.getOrElse("AcumeTreeCacheValue", new ArrayBuffer[AcumeTreeCacheValue]).asInstanceOf[ArrayBuffer[AcumeTreeCacheValue]]
    list.+=(acumeTreeCacheValue)
    threadLocal.get().put("AcumeTreeCacheValue", list)
  }
  
  def setQueryTable(tableName : String) {
    val list = threadLocal.get.getOrElse("QueryTable", new ArrayBuffer[String]).asInstanceOf[ArrayBuffer[String]]
    list.+=(tableName)
    threadLocal.get().put("QueryTable", list)
  }
  
  def setInstaTempTable(tableName : String) {
    val list = threadLocal.get.getOrElse("InstaTempTable", new ArrayBuffer[String]).asInstanceOf[ArrayBuffer[String]]
    list.+=(tableName)
    threadLocal.get().put("InstaTempTable", list)
  }
  
  def setSparkSqlShufflePartitions(numPartitions: String) {
    threadLocal.get.put(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS, numPartitions)
  }
  
  def getSparkSqlShufflePartitions(): String = {
    threadLocal.get.get(AcumeConstants.SPARK_SQL_SHUFFLE_PARTITIONS).get.asInstanceOf[String]
  }

  def unsetAcumeTreeCacheValue() {
    threadLocal.get.remove("AcumeTreeCacheValue")
  }
  
  def unsetQueryTable(cacheContext : AcumeCacheContextTrait) {
    val x = threadLocal.get.remove("QueryTable").map(x => {
      x.asInstanceOf[ArrayBuffer[String]].map(cacheContext.cacheSqlContext.dropTempTable(_))
    })
  }
  
  def getInstaTempTable() = {
    threadLocal.get.remove("InstaTempTable")
  }
  
  def unsetAll(cacheContext : AcumeCacheContextTrait) {
    unsetQueryTable(cacheContext)
    getInstaTempTable
    unsetAcumeTreeCacheValue
  }
  
  
}