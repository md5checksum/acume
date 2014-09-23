package com.guavus.acume.disk.utility

import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.io.NullWritable
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row
import com.guavus.acume.gen.Acume
import com.guavus.crux.core.Fields
import com.guavus.crux.core.WritableTuple
import com.guavus.crux.df.operations.core.CopyAnnotation
import com.guavus.crux.df.stream._
import com.guavus.crux.df.operations.wrappers._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import com.guavus.acume.core.CacheLevel
import com.guavus.acume.configuration.AcumeConfiguration
import java.util.UUID
import java.util.Random


class DiskReader(sqlContext: SQLContext, acumeCube: Acume) { 
  
  val baseCubeUtility: BaseCubeUtility = new BaseCubeUtility()
  val businessCubeUtility: BusinessCubeUtility = new BusinessCubeUtility()
  val businessCubeAggregationUtility: BusinessCubeAggregationUtility = new BusinessCubeAggregationUtility();
  def initDiskReader(sqlContext: SQLContext, acumeCube: Acume) = { 
    
//    val aggregated_data = sqlContext.sql("select " + businessCubeDimensionList + ", " + businessCubeAggregatedMeasureList + " from " + ORC+"base" + " groupBy " + businessCubeDimensionList)
//    val rdd = aggregated_data.map(x => new WritableTuple(x.map(y =>y).toArray))
//    val stream  = new com.guavus.crux.df.operations.wrappers.Transform("x", new Stream(new StreamMetaData("inname", "junk", new Fields((businessCubeDimensionList++businessCubeMeasureList).toArray)), rdd).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
//    val finalRdd = stream.iterator.next.rdd.map(x=>new GenericRow(x.getValueArray))
      
  }
  
  def getRDDLineage(businessCubeName: String, level: CacheLevel, timestamp: Long, globalDTableName: String) = { 

    
    val baseCube = baseCubeUtility.getORCMap(acumeCube.getCubes().getCube().toList).get(businessCubeName)
    val cube = 
      baseCube match { 
      
      case Some(x) => x
      case None => throw new RuntimeException("Value not found.")
    }
    val baseDir = AcumeConfiguration.ORCBasePath.getValue() + "/" + AcumeConfiguration.InstaInstanceId.getValue() + "/" + "bin-class" + "/" + "base-level" + "/" + cube + "/f/" + timestamp
    // orc/parquet support
    val sparkContext = sqlContext.sparkContext
    val rowRDD = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](baseDir).map(getRow)
    val schema = StructType(
        baseCubeUtility.getORCMFieldNames(cube).split(",").map(fieldName => { 
          StructField(fieldName.trim, baseCubeUtility.getMFieldType(fieldName.trim, cube), true)
        }))
    
    val thisCubeName = cube + getUniqueRandomeNo
    val schemaRDD = sqlContext.applySchema(rowRDD, schema).registerTempTable(thisCubeName)
    
    joinDimensionSet(businessCubeName, level, timestamp, globalDTableName, thisCubeName)
    
    
//    val baseCubeDimensionList = businessCubeUtility.getRequiredBaseDimensions();
//    
//    val baseCubeAggregatedMeasureList = businessCubeAggregationUtility.getRequiredBaseAggregatedMeasures();
//    val baseCubeAggregatedMeasureAliasList = businessCubeAggregationUtility.getAggregatedMeasureAlias();
//    val baseCubeMeasureList = businessCubeUtility.getRequiredBaseMeasures();
//    
    //explore hive udfs for aggregation.
    
//    val aggregatedRDD = sqlContext.sql("select " + baseCubeDimensionList + ", " + baseCubeAggregatedMeasureList + " from " + thisCubeName + " groupBy " + baseCubeDimensionList)
    //check if there is a better way to compute aggregatedRDD.
//    val annotatedRDD = aggregatedRDD.map(x => { 
//      new WritableTuple(x.toArray) 
//      })
    
    //remove dependency from crux. write things at acume level. 	
    
//    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
    
  }
  
  def loadDimensionSet(businessCubeName: String, timestamp: Long, globalDTableName: String): Boolean = { 
    
    // This loads the dimension set of cube businessCubeName for the particular timestamp into globalDTableName table.
    try { 
    val baseCubeUtility: BaseCubeUtility = new BaseCubeUtility()
    val businessCubeUtility: BusinessCubeUtility = new BusinessCubeUtility()
    val businessCubeAggregationUtility: BusinessCubeAggregationUtility = new BusinessCubeAggregationUtility();
    
    val baseCube = baseCubeUtility.getORCMap(acumeCube.getCubes().getCube().toList).get(businessCubeName)
    val cube = 
      baseCube match { 
      
      case Some(x) => x
      case None => throw new RuntimeException("Value not found.")
    }
    
    val baseDir = AcumeConfiguration.ORCBasePath.getValue() + "/" + AcumeConfiguration.InstaInstanceId.getValue() + "/" + "bin-class" + "/" + "base-level" + "/" + cube + "/d/" + timestamp
    val sparkContext = sqlContext.sparkContext
    
    //only orc supported, check for parquet too.
    val rowRDD = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](baseDir).map(getRow)
    val schema = StructType(
        baseCubeUtility.getORCDFieldNames(cube).split(",").map(fieldName => { 
          StructField(fieldName.trim, baseCubeUtility.getDFieldType(fieldName.trim, cube), true)
        }))
        
    val thisCubeName = cube + getUniqueRandomeNo
    val schemaRDD = sqlContext.applySchema(rowRDD, schema)
    schemaRDD.registerTempTable(thisCubeName)
    sqlContext.sql("select " + businessCubeUtility.getBusinessCubeDimensionList(businessCubeName) + s" from $thisCubeName").insertInto(globalDTableName)
    true
    } catch { 
      case ex => false 
      
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
  
  def joinDimensionSet(businessCubeName: String, level: CacheLevel, timestamp: Long, globalDTableName: String, thisCubeName: String) = { 
    
    val businessCubeAggregatedMeasureList = businessCubeAggregationUtility.getBusinessCubeAggregatedMeasureList(businessCubeName);
    val local_thisCubeName = thisCubeName + getUniqueRandomeNo
    loadDimensionSet(businessCubeName, timestamp, globalDTableName)
    val aggregatedRDD = sqlContext.sql("select " + thisCubeName + ".tupleid, " + businessCubeAggregatedMeasureList + " from " + thisCubeName + " groupBy " + businessCubeAggregatedMeasureList + ".tupleid").registerTempTable(local_thisCubeName)
    sqlContext.sql(s"Select * from $globalDTableName INNER JOIN $local_thisCubeName ON $globalDTableName.id = $local_thisCubeName.tupleid")
    
    
    
    //explore hive udfs for aggregation.
    //remove dependency from crux. write things at acume level. 	
    
//    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
    
  }
  def gotNewCube(timestamp: Long) = { 
    
    
  }
  
  def createBusinessCubes(fieldsNCubes: Acume) = { 
    
    val businessCubeMap = scala.collection.mutable.Map[String, List[String]]()
    val list = fieldsNCubes.getFields().getField()
    for(field <- list) { 
      
      
    }
    
    val cubeList = fieldsNCubes.getCubes.getCube()
    
    for(cube <- cubeList) { 
      
      val cubeName = cube.getName()
      val cubeFields = cube.getFields().split(",").map(_.trim).toList
      businessCubeMap += cubeName -> cubeFields
    }
    businessCubeMap
  }
}




