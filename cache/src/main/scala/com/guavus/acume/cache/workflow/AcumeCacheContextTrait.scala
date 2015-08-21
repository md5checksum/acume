package com.guavus.acume.cache.workflow

import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.disk.utility.DataLoader
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * @author archit.thakur
 * 
 */
abstract class AcumeCacheContextTrait(val cacheSqlContext : SQLContext, val cacheConf: AcumeCacheConf) extends Serializable {
  
  @transient
  private [cache] var rrCacheLoader : RRCache = Class.forName(cacheConf.get(ConfConstants.rrloader)).getConstructors()(0).newInstance(this, cacheConf).asInstanceOf[RRCache]
  private [cache] val dataLoader : DataLoader = null
	
  lazy private [cache] val measureMap = AcumeCacheContextTraitUtil.measureMap
	lazy private [cache] val dimensionMap = AcumeCacheContextTraitUtil.dimensionMap
  lazy private [cache] val cubeMap = AcumeCacheContextTraitUtil.cubeMap.filter(cubeKey => cubeKey._2.dataSource.equalsIgnoreCase(cacheConf.getDataSourceName))
  lazy private [cache] val cubeList = AcumeCacheContextTraitUtil.cubeList.filter(cube => cube.dataSource.equalsIgnoreCase(	cacheConf.getDataSourceName))

  cacheSqlContext match {
    case hiveContext: HiveContext =>
    case hbaseContext : HBaseSQLContext =>
    case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
  
  def acql(sql: String): AcumeCacheResponse = {
  AcumeCacheContextTraitUtil.setQuery(sql)
    try {
      if (cacheConf.getInt(ConfConstants.rrsize._1).get == 0) {
        executeQuery(sql)
      } else {
        rrCacheLoader.getRdd((sql))
      }
    } finally {
      AcumeCacheContextTraitUtil.unsetQuery()
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
  
  lazy private [acume] val getCubeMap : Map[CubeKey, Cube] = cubeMap.toMap

  lazy private[acume] val getCubeList : List[Cube] = cubeList.toList
  
  private [acume] def getCube(cube: CubeKey) = getCubeMap.get(cube).getOrElse(throw new RuntimeException(s"cube $cube not found."))
  
  /* To be overrided by subclasses */
  private [acume] def executeQuery(sql : String) : AcumeCacheResponse
  
  private[acume] def getFieldsForCube(name: String, binsource: String): List[String] = {
    val cube = getCubeMap.getOrElse(CubeKey(name, binsource), throw new RuntimeException(s"Cube $name Not in AcumeCache knowledge."))
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
        for(cube <- getCubeList if(dimensionSet.toSet.subsetOf(cube.dimension.dimensionSet.toSet) && 
            measureSet.toSet.subsetOf(cube.measure.measureSet.toSet))) yield {
          cube
        }
    kCube.toList
  }
  
}
