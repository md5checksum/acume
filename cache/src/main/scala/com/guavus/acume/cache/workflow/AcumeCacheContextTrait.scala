package com.guavus.acume.cache.workflow

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList

import org.apache.spark.sql.SQLContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.core.AcumeTreeCacheValue
import com.guavus.acume.cache.disk.utility.DataLoader
import com.guavus.acume.cache.utility.InsensitiveStringKeyHashMap
import com.guavus.acume.cache.utility.Utility

 
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
  private [cache] val dataLoader : DataLoader = null
  private [cache] val dataloadermap : ConcurrentHashMap[String, DataLoader] = new ConcurrentHashMap[String, DataLoader]
  private [cache] val cubeMap = new HashMap[CubeKey, Cube]
  private [cache] val cubeList = MutableList[Cube]()

  private [acume] val cacheSqlContext : SQLContext
  private [acume] val cacheConf: AcumeCacheConf
  Utility.init(cacheConf)
  
  def acql(sql: String): AcumeCacheResponse = {
  AcumeCacheContextTrait.setQuery(sql)
    try {
      if (cacheConf.getInt(ConfConstants.rrsize._1).get == 0) {
        executeQuery(sql)
      } else {
        rrCacheLoader.getRdd((sql))
      }
    } finally {
      AcumeCacheContextTrait.unsetQuery()
    }
  }
  
  def threadLocal: InheritableThreadLocal[HashMap[String, Any]] = poolThreadLocal
    
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
  
  private [acume] def getCubeMap : Map[CubeKey, Cube] = cubeMap.toMap

  private[acume] def getCubeList: List[Cube] = cubeList.toList
  
  private [acume] def getCube(cube: CubeKey) = cubeMap.get(cube).getOrElse(throw new RuntimeException(s"cube $cube not found."))
  
  /* To be overrided by subclasses */
  private [acume] def executeQuery(sql : String) : AcumeCacheResponse
  
  private[acume] def getFieldsForCube(name: String, binsource: String): List[String] = {
    val cube = cubeMap.getOrElse(CubeKey(name, binsource), throw new RuntimeException(s"Cube $name Not in AcumeCache knowledge."))
    cube.dimension.dimensionSet.map(_.getName) ++ cube.measure.measureSet.map(_.getName)
  }

  private[acume] def getAggregationFunction(stringname: String): String = {
    val measure = measureMap.getOrElse(stringname, throw new RuntimeException(s"Measure $stringname not in Acume knowledge."))
    measure.getAggregationFunction
  }

  private[acume] def getCubeListContainingFields(lstfieldNames: List[String]): List[Cube] = {
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
  
  def getFirstBinPersistedTime(binSource : String) : Long =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getLastBinPersistedTime(binSource : String) : Long =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getBinSourceToIntervalMap(binSource : String) : Map[Long, (Long,Long)] =  {
    throw new NoSuchMethodException("Method not present")
  }
  
  def getAllBinSourceToIntervalMap() : Map[String, Map[Long, (Long,Long)]] =  {
    throw new NoSuchMethodException("Method not present")
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
