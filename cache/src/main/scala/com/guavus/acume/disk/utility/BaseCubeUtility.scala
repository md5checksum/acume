package com.guavus.acume.disk.utility

import com.guavus.acume.gen.Acume.Cubes.Cube
import org.apache.spark.sql._

class BaseCubeUtility {

  def getSize(baseCubeName: String): Int = { 
    
    //returns the no. of fields in the cube.
    //will come from base cube xml.
    0
  }
  def getORCDFieldNames(baseCubeName: String): String = { 
    
    //returns the (comma separated) field names. 
    null
  }
  def getORCMFieldNames(baseCubeName: String): String = { 
    
    //returns the (comma separated) field names. 
    null
  }
  def getDFieldType(fieldname: String, cubeName: String): DataType = { 
    
    ///returns the field types.
    null
  }
  def getMFieldType(fieldname: String, cubeName: String): DataType = { 
    
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
    
    //returns the map of business cube vs. base cube of orc. 	
    //TBD
    null
  }
  def getORCList(businessCubeList: List[Cube]): List[String] = { 
    
    //returns the list of orc's to be read.  
    //TBD
    null
  }
}