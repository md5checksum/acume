package com.guavus.acume.cache.disk.utility

class BusinessCubeUtility {

  def getRequiredBaseDimensions(): List[String] = { 
    
    //returns the comma separated base dimensions required in the cube. 	
    
    null
  }
  
  def getRequiredBaseMeasures(): List[String] = { 
    
    //returns the comma separated base measures required in the cube.
    null
  }
  
  def getBusinessCubeDimensionList(businessCubeName: String) = { 
    
    //returns the comma speparated dimension list for businessCube cube.
  }
  
  def getBusinessCubeMeasureList(businessCubeName: String) = { 
    
    //returns the comma speparated measure list for businessCube cube.
  }
  
  def getBusinessCubeFieldsString(cubeName: String): String = { 
    
    
    //returns the business cube fields (comma separated) of cubeName cube.
    null
  }
}

class BusinessCubeAggregationUtility { 
  
  def getBusinessCubeAggregatedMeasureList(businessCubeName: String): List[String] = { 
    
    //returns the comma separated business measures required in the cube.
    //eg, sum(M1), avg(M2) ... or some other aggregator etc etc.
    null
  }
  
  def getAggregatedMeasureAlias(): List[String] = { 
    
    //returns the comma separated base measures required in the cube.
    //eg, sum(M1) as Mx1, avg(M2) as Mx2 ... or some other aggregator etc etc.
    //returns Mx1, Mx2 etc.
    null
  }
}