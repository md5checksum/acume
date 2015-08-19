package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import AcumeCacheContextTrait._
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.QLType
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.utility.InsensitiveStringKeyHashMap
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.disk.utility.DataLoader
import java.util.concurrent.ConcurrentHashMap
import com.guavus.acume.cache.common.BaseCube
import scala.collection.mutable.MutableList
 
/**
 * @author archit.thakur
 * 
 */
trait AcumeCacheContextTrait extends Serializable {
  
  @transient
  private [cache] var rrCacheLoader : RRCache = Class.forName(cacheConf.get(ConfConstants.rrloader)).getConstructors()(0).newInstance(this, cacheConf).asInstanceOf[RRCache]
  private [cache] val dimensionMap = new InsensitiveStringKeyHashMap[Dimension]
  private [cache] val measureMap = new InsensitiveStringKeyHashMap[Measure]
  private [cache] val poolThreadLocal = new InheritableThreadLocal[HashMap[String, Any]]()
  private [cache] val baseCubeList = MutableList[BaseCube]()
  private [cache] val cubeMap = new HashMap[CubeKey, Cube]
  private [cache] val cubeList = MutableList[Cube]()
  
  val dataLoader : DataLoader = null
  
  def acql(sql: String): AcumeCacheResponse = {
    acql(sql, null)
  }

  def acql(sql: String, qltype: String): AcumeCacheResponse = {
    setQuery(sql)
    try {
      val ql: QLType.QLType = if (qltype == null)
        QLType.getQLType(cacheConf.get(ConfConstants.qltype))
      else
        QLType.getQLType(qltype)

      validateQLType(ql)

      if (cacheConf.getInt(ConfConstants.rrsize._1) == 0) {
        executeQuery(sql, ql)
      } else {
        rrCacheLoader.getRdd((sql, ql))
      }
    } finally {
      unsetQuery()
    }
  }
  
  def threadLocal: InheritableThreadLocal[HashMap[String, Any]] = poolThreadLocal
    
  private [cache] def validateQLType(qltype: QLType.QLType) = {
    if (!checkQLValidation(cacheSqlContext, qltype))
      throw new RuntimeException(s"ql not supported with ${cacheSqlContext}");
  }
  
  private [cache] def checkQLValidation(sqlContext: SQLContext, qltype: QLType.QLType) = { 
    sqlContext match{
      case hiveContext: HiveContext =>
        qltype match {
          case QLType.hql | QLType.sql => true
          case rest => false
        }
      case sqlContext: SQLContext => 
        qltype match {
          case QLType.sql => true
          case rest => false
        }
    }
  }
  
  def isDimension(name: String) : Boolean =  {
    if(dimensionMap.contains(name)) {
      true 
    } else if(measureMap.contains(name)) {
      false
    } else {
        throw new RuntimeException("Field " + name + " nither in Dimension Map nor in Measure Map.")
    }
  }
  
  def getDefaultValue(fieldName: String) : Any = {
    if(isDimension(fieldName))
      dimensionMap.get(fieldName).get.getDefaultValue
    else
      measureMap.get(fieldName).get.getDefaultValue
  }
  
  private [acume] def getCubeList = cubeList.toList
  
  private [acume] def getFieldsForCube(name: String, binsource: String) = {
      
    val cube = cubeMap.getOrElse(CubeKey(name, binsource), throw new RuntimeException(s"Cube $name Not in AcumeCache knowledge."))
    cube.dimension.dimensionSet.map(_.getName) ++ cube.measure.measureSet.map(_.getName)
  }
  
  private [acume] def getAggregationFunction(stringname: String) = {
    val measure = measureMap.getOrElse(stringname, throw new RuntimeException(s"Measure $stringname not in Acume knowledge."))
    measure.getAggregationFunction
  }
  
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]) = {
    
    val dimensionSet = scala.collection.mutable.Set[Dimension]()
    val measureSet = scala.collection.mutable.Set[Measure]()
    for(field <- lstfieldNames)
      if(isDimension(field))
        dimensionSet.+=(dimensionMap.get(field).get)
      else
        measureSet.+=(measureMap.get(field).get)
      val kCube = 
        for(cube <- cubeList if(dimensionSet.toSet.subsetOf(cube.dimension.dimensionSet.toSet) && 
            measureSet.toSet.subsetOf(cube.measure.measureSet.toSet))) yield {
          cube
        }
    kCube.toList
  }
  
  private [acume] def getCubeMap: Map[CubeKey, Cube] = cubeMap.toMap
  
  private [acume] def executeQuery(sql : String, qltype : QLType.QLType) : AcumeCacheResponse
  
  private [acume] def cacheConf : AcumeCacheConf
  
  private [acume] def cacheSqlContext() : SQLContext
  
  def getFirstBinPersistedTime(binSource: String): Long = {
    dataLoader.getFirstBinPersistedTime(binSource)
  }
  
  def getLastBinPersistedTime(binSource: String): Long = {
    dataLoader.getLastBinPersistedTime(binSource)
  }
  
  def getBinSourceToIntervalMap(binSource: String): Map[Long, (Long, Long)] = {
    dataLoader.getBinSourceToIntervalMap(binSource)
  }
  
  def getAllBinSourceToIntervalMap() : Map[String, Map[Long, (Long,Long)]] =  {
    dataLoader.getAllBinSourceToIntervalMap
  }
  
}


object AcumeCacheContextTrait {
  
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
