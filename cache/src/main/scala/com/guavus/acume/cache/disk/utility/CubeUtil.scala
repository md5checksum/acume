package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.common.DataType._
import com.guavus.acume.cache.common._
import com.guavus.acume.cache.common.FieldType.FieldType
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.Field
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.workflow.AcumeCacheContext
import scala.collection.mutable.MutableList

object CubeUtil {

  val cube = new HashMap[Cube, BaseCube]
  
  def getSize(cube: CubeTrait): Int = cube.superDimension.dimensionSet.size + cube.superMeasure.measureSet.size
  
  def getDimensionSet(cube: CubeTrait): Set[Dimension] = cube.superDimension.dimensionSet
  
  def getMeasureSet(cube: CubeTrait): Set[Measure] = cube.superMeasure.measureSet
  
  def getFieldType(field: Field): DataType = field.getDataType
  
  def getCubeFields(cube: CubeTrait) = cube.superDimension.dimensionSet.map(_.getName) ++ cube.superMeasure.measureSet.map(_.getName)
  
  def getCubeMap(businessCubeList: List[Cube]): Map[Cube, BaseCube] = { 
    
    var flag = true
    val tupleListNotFound = 
      for(k <- businessCubeList) yield
      if(cube.contains(k)) (true, k)
      else (false, k)
    val baseCubeList = AcumeCacheContext.baseCubeList
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
        if(dimensionSet.subsetOf(baseCubeDimensionSet) && measureSet.subsetOf(baseCubeMeasureSet)){
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
  
  def getORCList(businessCubeList: List[Cube]): List[String] = { 
    
    //returns the list of orc's to be read.  
    //TBD
    null
  }
  
  def getStringMeasureOrFunction(cube: CubeTrait): String = { 
    
    //returns the comma separated business measures required in the cube.
    //eg, sum(M1), avg(M2) ... or some other aggregator etc etc.
    val fieldMap = AcumeCacheContext.measureMap
    val keyItr = for(key <- fieldMap.keys) yield s"${fieldMap(key).getFunction.functionName}($key) as $key"
    keyItr.mkString(",")
  }
}