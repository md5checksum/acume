package com.guavus.acume.disk.utility

import com.guavus.acume.gen.Acume.Cubes.Cube
import org.apache.spark.sql._

class BaseCubeUtility {

  def getSize(cubeName: String): Int = { 
    
    //return sthe no. of fields in the cube.
    0
  }
  def getORCFieldNames(cubeName: String): String = { 
    
    //returns the (comma separated) field names. 
    null
  }
  def getFieldType(fieldname: String, cubeName: String): DataType = { 
    
    ///returns the field types.
    null
  }
  def getORCFieldTypes(cubeName: String) = { 
    
    ///returns the field types.
  }
  def getORCSchema(cubeName: String) = { 
    
    //returns the schema of the orc file or the cube specified, could be changed.
  }
  def getORCMap(businessCubeList: List[Cube]): Map[String, String] = { 
    
    //returns the map of business cube vs. orc's to be read.  
    //TBD
    null
  }
  def getORCList(businessCubeList: List[Cube]): List[String] = { 
    
    //returns the list of orc's to be read.  
    //TBD
    null
  }
}