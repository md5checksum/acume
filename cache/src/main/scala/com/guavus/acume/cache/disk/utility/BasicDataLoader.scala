package com.guavus.acume.cache.disk.utility

import java.util.Random
import scala.Array.canBuildFrom
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.StructField
import org.apache.spark.sql.StructType
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.LongType
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.ConversionToCrux
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.DataType
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.crux.core.Fields
import com.guavus.crux.df.core.FieldDataType.FieldDataType
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.common.DimensionTable

abstract class BasicDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) extends DataLoader(acumeCacheContext, conf, cube) { 
  
  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, DTableName: DimensionTable) = { 
    
    val instabase = conf.get(ConfConstants.instabase)
    val instainstanceid = conf.get(ConfConstants.instainstanceid)
    loadData(businessCube, levelTimestamp, DTableName, instabase, instainstanceid)
  }
  
  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, globalDTableName: DimensionTable, instabase: String, instainstanceid: String) = { 
    
    val list = getLevel(levelTimestamp).toList //list of timestamps to be loaded on base gran, improve this to support grans in insta .
    val baseCube = CubeUtil.getCubeMap(acumeCacheContext.baseCubeList.toList, acumeCacheContext.cubeList.toList).getOrElse(businessCube, throw new RuntimeException("Value not found."))
    val baseDimensionSetTable = baseCube.cubeName + "dimensionset"
    val baseMeasureSetTable = baseCube.cubeName + "measureset" +levelTimestamp
    val level = levelTimestamp.level
    loadMeasureSet(baseCube, list, baseMeasureSetTable, instabase, instainstanceid)
    loadDimensionSet(baseCube, list, baseDimensionSetTable, instabase, instainstanceid)
    modifyDimensionSet(baseCube, businessCube, baseDimensionSetTable, globalDTableName, instabase, instainstanceid)
    val joinDimMeasureTableName = baseMeasureSetTable + getUniqueRandomeNo
    dMJoin(globalDTableName, baseMeasureSetTable, joinDimMeasureTableName)
    getSchemaRDD(businessCube, joinDimMeasureTableName)
  }
  
  private def dMJoin(globalDTableName: DimensionTable, baseMeasureSetTable: String, finalName: String) = { 
    
    import acumeCacheContext.sqlContext._
    val sqlContext = acumeCacheContext.sqlContext

    val join = s"Select * from ${globalDTableName.tblnm} INNER JOIN $baseMeasureSetTable ON id = tupleid"
    val globaldtblnm = globalDTableName.tblnm
    val globalDTable = table(globaldtblnm)
    sqlContext.applySchema(globalDTable, globalDTable.schema).registerTempTable(globaldtblnm)
    val joinedRDD = sqlContext.sql(join)
    joinedRDD.registerTempTable(finalName)
  }
  
  private def getSchemaRDD(businessCube: Cube, joinDimMeasureTableName: String) = { 
    
    import acumeCacheContext.sqlContext._
    val sqlContext = acumeCacheContext.sqlContext
    val measureMapThisCube = acumeCacheContext.measureMap.clone.filterKeys(key => businessCube.measure.measureSet.contains(acumeCacheContext.measureMap.get(key).get)) .toMap
    val businessCubeAggregatedMeasureList = CubeUtil.getStringMeasureOrFunction(measureMapThisCube, cube)
    val businessCubeDimensionList = CubeUtil.getDimensionSet(cube).map(_.getName).mkString(",")
    val str = "select " + businessCubeDimensionList + "," + businessCubeAggregatedMeasureList + " from " + joinDimMeasureTableName + " group by " + businessCubeDimensionList
    val xRDD = sqlContext.sql(str)
    xRDD.collect.map(println)
    xRDD
    
    //explore hive udfs for aggregation.
    //remove dependency from crux. write things at acume level. 	
//    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
  }
  
  def getRowSchemaRDD(sqlContext: SQLContext, baseDir: String, fields: Fields, datatypearray: Array[FieldDataType]): RDD[Row] 
  
  private def getLevel(levelTimestamp: LevelTimestamp) = CubeUtil.getLevel(levelTimestamp)
  
  private def getUniqueRandomeNo: String = System.currentTimeMillis() + "" + Math.abs(new Random().nextInt)
  
  private def getRow(row: String) = Row.fromSeq(row.split("\t").toSeq)
  
  private def loadMeasureSet(baseCube: BaseCube, list:List[Long], baseMeasureSetTable: String, instabase: String, instainstanceid: String) = {
    
    val sqlContext = acumeCacheContext.sqlContext
    var flag = false

    val schema = CubeUtil.getMeasureSet(baseCube).map(field => { 
            StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
          })
    val latestschema = StructType(StructField("tupleid", LongType, true) +: StructField("ts", LongType, true) +: schema.toList)
          
    val baseCubeMeasureSet = CubeUtil.getMeasureSet(baseCube)
    val fields  = new Fields((1.to(baseCubeMeasureSet.size + 2).map(_.toString).toArray))
    val datatypearray = Array(ConversionToCrux.convertToCruxFieldDataType(DataType.ACLong), ConversionToCrux.convertToCruxFieldDataType(DataType.ACLong))  ++ baseCubeMeasureSet.map(x => ConversionToCrux.convertToCruxFieldDataType(x.getDataType))
    val _$list = for(ts <- list) yield {
    
      val baseDir = instabase + "/" + instainstanceid + "/" + "bin-class" + "/" + "base-level" + "/" + baseCube.cubeName + "/f/" + ts
      val rowRDD = getRowSchemaRDD(sqlContext, baseDir, fields, datatypearray)
      val schemaRDD = acumeCacheContext.sqlContext.applySchema(rowRDD, latestschema)
      schemaRDD
    }
    if(!_$list.isEmpty)
      sqlContext.applySchema(_$list.map(_.asInstanceOf[RDD[Row]]).reduce(_.union(_)), latestschema).registerTempTable(baseMeasureSetTable)
    else
      Utility.getEmptySchemaRDD(sqlContext, latestschema).registerTempTable(baseMeasureSetTable)
    true
  }
  
  private def modifyDimensionSet(baseCube: BaseCube, businessCube: Cube, 
      baseDimensionSetTable: String, globalDTableName: DimensionTable, 
      instabase: String, instainstanceid: String) = {
    
    val sqlContext = acumeCacheContext.sqlContext
    val getdimension = CubeUtil.getDimensionSet(businessCube).map(_.getName).mkString(",")
    val baseDimensionSetTableNew = s"${baseDimensionSetTable}New"
    val baseDimensionSetTableCombined = s"${baseDimensionSetTable}Combined"
    val dimensionSQL = s"select id, timestamp, ${getdimension} from $baseDimensionSetTableCombined"
    val globaldtblnm = globalDTableName.tblnm
    import acumeCacheContext.sqlContext._
    val istableregistered = 
      try{
        table(globaldtblnm)
        true
      } catch{
      case ex: Exception => false
      }
      
      //get it from insta service not from config.
      val completelist = conf.getOption(ConfConstants.completelist).getOrElse("").split(",").filter(!_.isEmpty).map(_.trim).map(_.toLong).toList
      val unLoadedList = completelist.filterNot(acumeCacheContext.dimensionTimestampLoadedList.toSet)
      //completelist.split(",").map(_.trim).map(_.toLong).toList
      loadDimensionSet(baseCube, unLoadedList, s"$baseDimensionSetTableNew", instabase, instainstanceid)
      sqlContext.sql(s"select * from $baseDimensionSetTableNew UNION ALL select * from $baseDimensionSetTable").registerTempTable(s"$baseDimensionSetTableCombined")
      
      val dimensionRDD = sqlContext.sql(dimensionSQL)
      println(dimensionRDD.schema)
      dimensionRDD.collect.map(println)
      sqlContext.applySchema(dimensionRDD, dimensionRDD.schema)
      if(istableregistered) {
//        val dimrdd = sqlContext.sql(s"select * from $globalDTableName")
//        dimrdd.collect.map(println)
//        println(dimrdd.schema)
        val unioned = table(globaldtblnm).union(dimensionRDD)
        globalDTableName.tblnm = globalDTableName.tblnm+"new"
        sqlContext.applySchema(unioned, dimensionRDD.schema).registerTempTable(globalDTableName.tblnm)
//        dimensionRDD.insertInto(globaldtblnm)
      }
      else 
        dimensionRDD.registerTempTable(globaldtblnm)
  }
  
  private def loadDimensionSet(baseCube: BaseCube, list: List[Long], baseDimensionSetTable: String, instabase: String, instainstanceid: String): Boolean = { 
    
    // This loads the dimension set of cube businessCubeName for the particular timestamp into globalDTableName table.
    try { 
      val sqlContext = acumeCacheContext.sqlContext
      val sparkContext = sqlContext.sparkContext
      
      val baseCubeDimensionSet = CubeUtil.getDimensionSet(baseCube)
      val schema = 
        baseCubeDimensionSet.map(field => { 
          StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
        })
          
      val latestschema = StructType(StructField("id", LongType, true) +: StructField("timestamp", LongType, true) +: schema.toList)
      val fields  = new Fields((1.to(baseCubeDimensionSet.size + 2).map(_.toString).toArray))
      val datatypearray = Array(ConversionToCrux.convertToCruxFieldDataType(DataType.ACLong), ConversionToCrux.convertToCruxFieldDataType(DataType.ACLong)) ++ baseCubeDimensionSet.map(x => ConversionToCrux.convertToCruxFieldDataType(x.getDataType))

      Utility.getEmptySchemaRDD(sqlContext, latestschema).registerTempTable(baseDimensionSetTable)//sqlContext.sql(s"select * from $baseDimensionSetTable").collect
      var flag = false
      val _list = for(timestamp <- list) yield {
        val baseDir = instabase + "/" + instainstanceid + "/" + "bin-class" + "/" + "base-level" + "/" + baseCube.cubeName + "/d/" + timestamp
        val rowRDD = getRowSchemaRDD(sqlContext, baseDir, fields, datatypearray)
//        val schemaRDD = sqlContext.applySchema(rowRDD, latestschema)
//      
//        if(!flag) { 
//      
//          schemaRDD.registerTempTable(baseDimensionSetTable)
//          flag = true
//        } else
//          schemaRDD.insertInto(baseDimensionSetTable)
//          
        acumeCacheContext.dimensionTimestampLoadedList.+=(timestamp)
        rowRDD
      }
      if(!_list.isEmpty)
        sqlContext.applySchema(_list.reduce(_.union(_)), latestschema).registerTempTable(baseDimensionSetTable)
    } catch { 
    case ex: Throwable => throw new IllegalStateException(ex)
    }
    true
  }
}



