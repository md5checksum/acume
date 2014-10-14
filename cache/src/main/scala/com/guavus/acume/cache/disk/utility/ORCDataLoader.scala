package com.guavus.acume.cache.disk.utility

import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row
import com.guavus.acume.cache.gen.Acume
import com.guavus.crux.core.Fields
import com.guavus.crux.core.WritableTuple
import com.guavus.crux.df.operations.core.CopyAnnotation
import com.guavus.crux.df.stream._
import com.guavus.crux.df.operations.wrappers._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import com.guavus.acume.cache.common.CacheLevel
import com.guavus.acume.cache.common.CacheLevel._
import com.guavus.acume.cache.common.AcumeCacheConf
import java.util.UUID
import java.util.Random
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.ConversionToSpark

class ORCDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) extends DataLoader(acumeCacheContext, conf, cube) { 
  
  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, DTableName: String) = { 
    
    val instabase = conf.get(ConfConstants.instabase)
    val instainstanceid = conf.get(ConfConstants.instainstanceid)
    loadData(businessCube, levelTimestamp, DTableName, instabase, instainstanceid)
  }
  
  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, DTableName: String, instabase: String, instainstanceid: String) = { 
    
    val level = levelTimestamp.level
    val timestamp = levelTimestamp.timestamp
    val baseCube = CubeUtil.getCubeMap(acumeCacheContext.baseCubeList.toList, acumeCacheContext.cubeList.toList).getOrElse(businessCube, throw new RuntimeException("Value not found."))
    val baseDir = instabase + "/" + instainstanceid + "/" + "bin-class" + "/" + "base-level" + "/" + baseCube + "/f/" + timestamp
    val sparkContext = acumeCacheContext.sqlContext.sparkContext
    val rowRDD = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](baseDir).map(getRow)
    val schema = StructType(
        CubeUtil.getMeasureSet(baseCube).toArray.map(field => { 
          StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
        }))
    
    val thisCubeName = baseCube + getUniqueRandomeNo
    val schemaRDD = acumeCacheContext.sqlContext.applySchema(rowRDD, schema).registerTempTable(thisCubeName)
    
    joinDimensionSet(businessCube, level, timestamp, DTableName, thisCubeName, instabase, instainstanceid)
    
    //explore hive udfs for aggregation.
    
//    val aggregatedRDD = sqlContext.sql("select " + baseCubeDimensionList + ", " + baseCubeAggregatedMeasureList + " from " + thisCubeName + " groupBy " + baseCubeDimensionList)
    //check if there is a better way to compute aggregatedRDD.
//    val annotatedRDD = aggregatedRDD.map(x => { 
//      new WritableTuple(x.toArray) 
//      })
    
    //remove dependency from crux. write things at acume level. 	
    
//    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
    
  }
  
  def loadDimensionSet(businessCube: Cube, timestamp: Long, instabase: String, instainstanceid: String, globalDTableName: String): Boolean = { 
    
    // This loads the dimension set of cube businessCubeName for the particular timestamp into globalDTableName table.
    try { 
      val sqlContext = acumeCacheContext.sqlContext
      val sparkContext = sqlContext.sparkContext
      
      val baseCube = CubeUtil.getCubeMap(acumeCacheContext.baseCubeList.toList, acumeCacheContext.cubeList.toList).getOrElse(businessCube, throw new RuntimeException("Value not found."))
      val baseDir = instabase + "/" + instainstanceid + "/" + "bin-class" + "/" + "base-level" + "/" + baseCube + "/d/" + timestamp

      val rowRDD = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](baseDir).map(getRow)
      val schema = StructType(
          CubeUtil.getDimensionSet(baseCube).toArray.map(field => { 
            StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
          }))
          
      val thisCubeName = baseCube + getUniqueRandomeNo
      val schemaRDD = sqlContext.applySchema(rowRDD, schema)
      schemaRDD.registerTempTable(thisCubeName)
      sqlContext.sql(s"select ${CubeUtil.getDimensionSet(businessCube).map(_.getName).mkString(",")} from $thisCubeName").insertInto(globalDTableName)
      true
    } catch { 
    case ex: Throwable => false   
    }
  }
  
  def getUniqueRandomeNo: String = System.currentTimeMillis() + "" + new Random().nextInt() 	
  
  def getRow(tuple: (NullWritable, OrcStruct)) = {
  
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val tokenList = field.substring(0, field.length - 2).split(',').map(_.trim)
    Row(tokenList)
  }
  
  def joinDimensionSet(businessCube: Cube, level: CacheLevel, timestamp: Long, globalDTableName: String, thisCubeName: String, instabase: String, instainstanceid: String) = { 
    
    val sqlContext = acumeCacheContext.sqlContext
    val businessCubeAggregatedMeasureList = CubeUtil.getStringMeasureOrFunction(acumeCacheContext.measureMap.toMap, cube)
    val local_thisCubeName = thisCubeName + getUniqueRandomeNo
    loadDimensionSet(businessCube, timestamp, instabase, instainstanceid, globalDTableName)
    val aggregatedRDD = sqlContext.sql("select " + thisCubeName + ".tupleid, " + businessCubeAggregatedMeasureList + " from " + thisCubeName + " groupBy " + thisCubeName + ".tupleid").registerTempTable(local_thisCubeName)
    sqlContext.sql(s"Select * from $globalDTableName INNER JOIN $local_thisCubeName ON $globalDTableName.id = $local_thisCubeName.tupleid")

    //explore hive udfs for aggregation.
    //remove dependency from crux. write things at acume level. 	
    
//    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
    
  }
}




