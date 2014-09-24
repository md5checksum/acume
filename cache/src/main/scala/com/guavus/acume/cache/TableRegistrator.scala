package com.guavus.acume.cache

import com.guavus.acume.utility.SQLHelper
import com.guavus.acume.common.AcumeConstants
import org.apache.spark.sql.SQLContext

object TableRegistrator {

  def createTable(sqx: String) = { 
    
    val list = SQLHelper.getTables(sqx)
  }

  def createTable(list: List[_]) = { 
    
    
  }
  
  private def createTableUtility(sqlContext: SQLContext, cubeName: String, startTime: Long, endTime: Long) = { 
    
    //list = list of table names.
    //timestamps = list of timestamps.
    
    import sqlContext._
//    val cube: List[String] = list
//    cube.map(cubeName => {
//    val jumpfactor = BIN_INTERVAL.HOURLY
    
    val localCubeName = cubeName.substring(0, cubeName.indexOf(AcumeConstants.TRIPLE_DOLLAR_SSC))
    sqlContext.sql(s"select * from $localCubeName+Dimension + _ + $startTime").registerTempTable("globalDimension")
    sqlContext.sql(s"select * from $localCubeName+Measure + _ + $startTime").registerTempTable("globalMeasure")
    //TO-DO: support other bin intervals as well.
    //TO-DO: support aggregation level, bin class as well.
    for(time <- startTime+3600 to endTime by 3600) { 
      val dimension = localCubeName+"Dimension" + "_" + time 
      val measure = localCubeName+"Measure" + "_" + time
      sqlContext.sql(s"select * from $dimension").insertInto("globalDimension")
      sqlContext.sql(s"select * from $measure").insertInto("globalMeasure")
      
    }
    
    sqlContext.sql(s"select * from globalDimension INNER JOIN globalMeasure ON globalDimension.id = globalMeasure.tupleid").registerTempTable(cubeName)
//    })
  }
  
  def aggregateByDimensions(list: List[String], measure: String) = { 
    
    
  }
}




