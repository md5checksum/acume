package com.guavus.acume.cache.disk.utility

import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.CubeTrait
import com.guavus.acume.cache.common.DataType.DataType
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Field
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.CubeMeasure
import com.guavus.acume.cache.core.TimeGranularity
import scala.collection.mutable.MutableList
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext

/**
 * @author archit.thakur
 *
 */
object CubeUtil {

  val cube = new HashMap[Cube, BaseCube]
  
  def getSize(cube: CubeTrait): Int = cube.superDimension.dimensionSet.size + cube.superMeasure.measureSet.size
  
  def getDimensionSet(cube: CubeTrait): List[Dimension] = cube.superDimension.dimensionSet
  
  def getMeasureSet(cube: CubeTrait): List[Measure] = cube.superMeasure.measureSet
  
  def getMeasureSetWithTimestampTupleId(cube: CubeTrait): List[Measure] = cube.superMeasure.measureSet
  
  def getFieldType(field: Field): DataType = field.getDataType
  
  def getCubeFields(cube: CubeTrait) = cube.superDimension.dimensionSet.map(_.getName) ++ cube.superMeasure.measureSet.map(_.getName)
  
  def getCubeBaseFields(cube: CubeTrait) = cube.superDimension.dimensionSet.map(_.getBaseFieldName) ++ cube.superMeasure.measureSet.map(_.getBaseFieldName)
  
  def getLevel(level: LevelTimestamp) = {
    
    //This should be moved inside metadataloader implementations.
    //this method returns the leveltimestamp which can serve the current leveltimestamp from metadataloader, currently insta it.
    val baselevel = TimeGranularity.HOUR // only hourly gran supported for insta layer as yet.
    val iLevel = level.level
    val iTs = level.timestamp
    val nextTs = iTs + iLevel.localId
    Utility.getAllIntervals(iTs, nextTs, baselevel.getGranularity)
  }
  
  def getCubeMap(baseCubeList: List[BaseCube], businessCubeList: List[Cube]): Map[Cube, BaseCube] = { 
    
    var flag = true
    val tupleListNotFound = 
      for(k <- businessCubeList) yield
      if(cube.contains(k)) (true, k)
      else (false, k)
    for(key <- tupleListNotFound if(!key._1)) { 
      
      val businessCube = key._2
      val dimensionSet = businessCube.dimension.dimensionSet
      val measureSet = businessCube.measure.measureSet
      val list = MutableList[BaseCube]()
      for(baseCube <- baseCubeList){
        val baseCubeDimensionSet = baseCube.dimension.dimensionSet
        val baseCubeMeasureSet = baseCube.measure.measureSet
        //todo how will you take care of derived measure here?
        //todo take care of annotated measure as well here.
        if(dimensionSet.toSet.subsetOf(baseCubeDimensionSet.toSet) && measureSet.toSet.subsetOf(baseCubeMeasureSet.toSet)){
          list.+=(baseCube)
        }
      }
      if(list.isEmpty){
        throw new RuntimeException(s"Cube Select not selection with the cube ${businessCube.cubeName}")
      }
      else{
        val baseCube = list.map(baseCube => (baseCube, getSize(baseCube))).min(Ordering[Int].on[(_,Int)](_._2))
        cube.put(businessCube, baseCube._1)
      }
    }
    cube.toMap
  } 	
  
  def getStringMeasureOrFunction(fieldMap: Map[String, Measure], businessCube: Cube): String = { 
    
    //returns the comma separated business measures required in the cube.
    //eg, sum(M1), avg(M2) ... or some other aggregator etc etc.
    val keyset = for(key <- fieldMap.keySet) yield {
      fieldMap.get(key) match {
      case None => ""
      case Some(measure) => 
        s"${measure.getAggregationFunction}($key) as ${key}"
      }
    }
    keyset.filter(!_.isEmpty()).toSet.+("min(ts) as ts ").mkString(",")
  }
  
  def getDimensionsAggregateMeasuresGroupBy(cube: Cube) = {
    val selectDimensions = CubeUtil.getDimensionSet(cube).map(_.getName).mkString(",")
    
    if(CubeUtil.getMeasureSet(cube).isEmpty) {
      throw new IllegalArgumentException("Measure set can't be empty for cube " + cube)
    }
    
    val measuresSize = CubeUtil.getMeasureSet(cube).size
    val noneMeasuresSize = CubeUtil.getMeasureSet(cube).filter(_.getAggregationFunction.equalsIgnoreCase("none")).size
    
    if(noneMeasuresSize == measuresSize) {
      val selectMeasures = CubeUtil.getMeasureSet(cube).map(_.getName).mkString(",")
      (selectDimensions, selectMeasures, "")
    } else if(noneMeasuresSize == 0) {
      val selectMeasures = CubeUtil.getMeasureSet(cube).map(x => x.getAggregationFunction + "('" + x.getName + ") as 'x").mkString(",")
      val groupBy = "group by $selectDimensions"
      (selectDimensions, selectMeasures, groupBy)
    } else {
      throw new IllegalArgumentException("Either all measures should have aggregation function as 'none' or none of them for cube " + cube)
    }
  }
}