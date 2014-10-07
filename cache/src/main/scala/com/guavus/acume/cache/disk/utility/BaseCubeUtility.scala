package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.common.DataType._
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.Field
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.workflow.AcumeCacheContext
import scala.collection.mutable.MutableList

class BaseCubeUtility {

  val cube = new HashMap[Cube, BaseCube]
  def getSize(baseCube: BaseCube): Int = { 
    
    //returns the no. of fields in the cube.
    baseCube.dimension.dimensionSet.size + baseCube.measure.measureSet.size
  }
  def getBaseCubeDimensionFieldNames(baseCube: BaseCube): Set[String] = { 
    
    //returns the (comma separated) field names. 
    baseCube.dimension.dimensionSet.map(_.getName)
  }
  def getBaseCubeMeasureFieldNames(baseCube: BaseCube): Set[String] = { 
    
    //returns the (comma separated) field names. 
    baseCube.measure.measureSet.map(_.getName)
  }
  def getFieldType(field: Field): DataType = { 
    
    ///returns the field types.
    field.getDataType
  }
  def getORCFieldTypes(cubeName: String) = { 
    
    ///returns the field types.
  }
  def getORCSchema(cubeName: String) = { 
    
    //returns the schema of the orc file or the cube specified, could be changed.
  }
  def getORCMap(businessCubeList: List[Cube]): Map[Cube, BaseCube] = { 
    
    var flag = true
    val tupleListNotFound = for(k <- businessCubeList) yield
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
}