package com.guavus.acume.cache.core

import com.guavus.acume.cache.common.DimensionTable
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.disk.utility.CubeUtil
import com.guavus.acume.cache.workflow.AcumeCacheContext

/**
 * @author archit.thakur
 *
 */
object AcumeCacheUtility {

  private [core] def dMJoin(sqlContext: SQLContext, globalDTableName: String, baseMeasureSetTable: String, finalName: String) = { 
    
    import sqlContext._
    val join = s"Select * from ${globalDTableName} INNER JOIN $baseMeasureSetTable ON id = tupleid"
    val globalDTable = table(globalDTableName)
    val joinedRDD = sqlContext.sql(join)
    joinedRDD.registerTempTable(finalName)
  }
    
  private [core] def getSchemaRDD(acumeCacheContext: AcumeCacheContext, cube: Cube, joinDimMeasureTableName: String) = { 
    
    val sqlContext = acumeCacheContext.cacheSqlContext
    import sqlContext._
    val measureMapThisCube = acumeCacheContext.measureMap.clone.filterKeys(key => cube.measure.measureSet.contains(acumeCacheContext.measureMap.get(key).get)) .toMap
    val businessCubeAggregatedMeasureList = CubeUtil.getStringMeasureOrFunction(measureMapThisCube, cube)
    val businessCubeDimensionList = CubeUtil.getDimensionSet(cube).map(_.getName).mkString(",")
    val str = "select " + businessCubeDimensionList + "," + businessCubeAggregatedMeasureList + " from " + joinDimMeasureTableName + " group by " + businessCubeDimensionList
    val xRDD = sqlContext.sql(str)
//    xRDD.collect.map(println)
//    xRDD.saveAsParquetFile("/data/archit//finalschemarddsaved")
    xRDD
    
    //explore hive udfs for aggregation.
//    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
  } 
}



