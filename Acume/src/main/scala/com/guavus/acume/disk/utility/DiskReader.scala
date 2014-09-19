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
import org.apache.spark.sql.catalyst.expressions.GenericRow


class DiskReader { 
  
  def initDiskReader(sqlContext: SQLContext, fieldsNCubes: Acume) = { 
    
    val sparkContext = sqlContext.sparkContext
    val baseCubeUtility: BaseCubeUtility = new BaseCubeUtility();
    val cubeORCMap = baseCubeUtility.getORCMap(fieldsNCubes.getCubes().getCube().toList)
    val businessCubeUtility: BusinessCubeUtility = new BusinessCubeUtility();
    //ORC support
    //could be extended to Parquet as well.
    //check it.
    import sqlContext._
    val baseRDDList = 
      for(tuple <- cubeORCMap) yield { 
        val cube = tuple._1
        val ORC = tuple._2
        val rowRDD = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](ORC).map(getRow)
        val businessCubeDimensionList = businessCubeUtility.getRequiredBaseDimensions();
        val businessCubeAggregatedMeasureList = businessCubeUtility.getRequiredBaseAggregatedMeasures();
        val businessCubeMeasureList = businessCubeUtility.getRequiredBaseMeasures();
        val schema = StructType(
            baseCubeUtility.getORCFieldNames(ORC).split(",").map(fieldName => { 
              StructField(fieldName.trim, baseCubeUtility.getFieldType(fieldName.trim, ORC), true)
            }))
        //explore hive udfs for aggregation.
        
        val schemaRDD = sqlContext.applySchema(rowRDD, schema)
        schemaRDD.registerTempTable(ORC+"base")
        val aggregated_data = sqlContext.sql("select " + businessCubeDimensionList + ", " + businessCubeAggregatedMeasureList + " from " + ORC+"base" + " groupBy " + businessCubeDimensionList)
//        schemaRDD.map(f)
        val rdd = aggregated_data.map(x => new WritableTuple(x.map(y =>y).toArray))
        
        val stream  = new com.guavus.crux.df.operations.wrappers.Transform("x", new Stream(new StreamMetaData("inname", "junk", new Fields((businessCubeDimensionList++businessCubeMeasureList).toArray)), rdd).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
        val finalRdd = stream.iterator.next.rdd.map(x=>new GenericRow(x.getValueArray))
      }
  }
  
  def getRow(tuple: (NullWritable, OrcStruct)) = {
  
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val tokenList = field.substring(0, field.length - 2).split(',').map(_.trim)
    Row(tokenList)
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