package com.guavus.acume.cache.disk.utility

import java.util.Random
import scala.Array.canBuildFrom
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.annotation.DeveloperApi
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
import com.guavus.acume.cache.common.DimensionTable
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.crux.core.Fields
import com.guavus.crux.df.core.FieldDataType.FieldDataType
import com.guavus.acume.cache.core.AcumeCache
import java.util.Calendar
import java.util.TimeZone
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.common.DimensionTable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait

/**
 * @author archit.thakur
 *
 */
abstract class BasicDataLoader(acumeCacheContextTrait: AcumeCacheContextTrait, conf: AcumeCacheConf, acumeCache: AcumeCache) extends DataLoader(acumeCacheContextTrait, conf, acumeCache) { 
  
  val acumeCacheContext = acumeCacheContextTrait.asInstanceOf[AcumeCacheContext]
  val logger: Logger = LoggerFactory.getLogger(classOf[BasicDataLoader])
  val cube = acumeCache.cube
  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, DTableName: DimensionTable) = { 
    
      val instabase = conf.get(ConfConstants.instabase)
      val instainstanceid = conf.get(ConfConstants.instainstanceid)
      val dataLoaded = loadData(businessCube, levelTimestamp, DTableName, instabase, instainstanceid)
      dataLoaded
  }
  
  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, globalDTableName: DimensionTable, instabase: String, instainstanceid: String) = { 
    
      val list = getLevel(levelTimestamp).toList //list of timestamps to be loaded on base gran, improve this to support grans in insta .
      val baseCube = CubeUtil.getCubeMap(acumeCacheContext.baseCubeList.toList, acumeCacheContext.cubeList.toList).getOrElse(businessCube, throw new RuntimeException("Value not found."))
      
      val baseDimensionSetTable = baseCube.cubeName + levelTimestamp + "dimensionset"
      val modifiedDimensionSetTable = baseCube.cubeName + levelTimestamp + "modifieddimensionset"
      
      val baseMeasureSetTable = baseCube.cubeName +levelTimestamp + "measureset"
      val level = levelTimestamp.level
      val sqlContext = acumeCacheContext.sqlContext
      import sqlContext._
      loadMeasureSet(baseCube, list, baseMeasureSetTable, instabase, instainstanceid)
      if(logger.isTraceEnabled)
        table(baseMeasureSetTable).collect.map(x => logger.trace(x.toString))
      this.synchronized {
        loadDimensionSet(baseCube, list, baseDimensionSetTable, instabase, instainstanceid)
        if(logger.isTraceEnabled)
          table(baseDimensionSetTable).collect.map(x => logger.trace(x.toString))
        modifyDimensionSet(baseCube, businessCube, baseDimensionSetTable, modifiedDimensionSetTable, globalDTableName, instabase, instainstanceid)
        if(logger.isTraceEnabled)
          table(modifiedDimensionSetTable).collect.map(x => logger.trace(x.toString))
        val modifiedmeasureset = modifyMeasureSet(baseCube, businessCube, baseMeasureSetTable, globalDTableName, instabase, instainstanceid)
        if(logger.isTraceEnabled)
          modifiedmeasureset._1.collect.map(x => logger.trace(x.toString))
        modifiedmeasureset
      }
  }
  
  def getRowSchemaRDD(sqlContext: SQLContext, baseDir: String, fields: Fields, datatypearray: Array[FieldDataType], schema: StructType): SchemaRDD 
  
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
      getRowSchemaRDD(sqlContext, baseDir, fields, datatypearray, latestschema)
      
    }
    if(!_$list.isEmpty)
      _$list.reduce(_.unionAll(_)).registerTempTable(baseMeasureSetTable)
    else
      Utility.getEmptySchemaRDD(sqlContext, latestschema).registerTempTable(baseMeasureSetTable)
    true
  }
  
  private def modifyMeasureSet(baseCube: BaseCube, businessCube: Cube, 
      baseMeasureSetTable: String, globalDTableName: DimensionTable, instabase: String, instainstanceid: String): (SchemaRDD, String) = {
    
    val sqlContext = acumeCacheContext.sqlContext
    val list = CubeUtil.getMeasureSet(businessCube).map(_.getName).mkString(",")
    val measuresql = s"select tupleid, ts, ${list} from $baseMeasureSetTable"
    import acumeCacheContext.sqlContext._
    val measurerdd = sqlContext.sql(measuresql)
    (measurerdd, globalDTableName.tblnm)
  }
  
  private def modifyDimensionSet(baseCube: BaseCube, businessCube: Cube, 
      baseDimensionSetTable: String, modifiedDimensionSetTable: String, globalDTableName: DimensionTable,  
      instabase: String, instainstanceid: String) = {
    
    val sqlContext = acumeCacheContext.sqlContext
    val getdimension = CubeUtil.getDimensionSet(businessCube).map(_.getName).mkString(",")
    val dimensionSQL = s"select id, timestamp, ${getdimension} from $baseDimensionSetTable"
    val globaldtblnm = globalDTableName.tblnm
    import acumeCacheContext.sqlContext._
    val istableregistered = 
      try{
        table(globaldtblnm)
        true
      } catch{
      case ex: Exception => false
      }
      
      val dimensionRDD = sqlContext.sql(dimensionSQL)
//      sqlContext.applySchema(dimensionRDD, dimensionRDD.schema)
      dimensionRDD.registerTempTable(modifiedDimensionSetTable)
      if(istableregistered) {
        cacheTable(modifiedDimensionSetTable)
        val unioned = sqlContext.sql("select * from " + globaldtblnm + " union all select * from " + modifiedDimensionSetTable)
//        val unioned = table(globaldtblnm).union(dimensionRDD)
        globalDTableName.Modify
        unioned.registerTempTable(globalDTableName.tblnm)
      }
      else {
        dimensionRDD.registerTempTable(globaldtblnm)
        cacheTable(globaldtblnm)
      }
  }
  
  private def loadDimensionSet(baseCube: BaseCube, list: List[Long], baseDimensionSetTable: String, instabase: String, instainstanceid: String): Boolean = { 
    
    // This loads the dimension set of cube businessCubeName for the particular timestamp into globalDTableName table.
    try { 
      val maxts = max(list).get
      
      val startts = DataLoader.getOrElseInsert(acumeCache, new DataLoadedMetadata)
      .getOrElseInsert(DataLoadedMetadata.dimensionSetStartTime, "0").toLong

      val endts = DataLoader.getOrElseInsert(acumeCache, new DataLoadedMetadata)
      .getOrElseInsert(DataLoadedMetadata.dimensionSetEndTime, "0").toLong
      
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
      val baseGran = cube.baseGran
      
      val dataloadedmetadata = DataLoader.getOrElseMetadata(acumeCache, new DataLoadedMetadata)
      
      val (startTime: Long, endTime: Long) = 
        if(startts == 0 && endts == 0) {
          dataloadedmetadata.put(DataLoadedMetadata.dimensionSetStartTime, conf.get(ConfConstants.firstbinpersistedtime))
          dataloadedmetadata.put(DataLoadedMetadata.dimensionSetEndTime, maxts.toString)
          (conf.get(ConfConstants.firstbinpersistedtime).toLong, maxts)
        }
        else if(maxts > endts) {
          
          dataloadedmetadata.put(DataLoadedMetadata.dimensionSetEndTime, maxts.toString)
          (Utility.getNextTimeFromGranularity(endts, baseGran.getGranularity, Calendar.getInstance(TimeZone.getTimeZone(ConfConstants.timezone))), maxts)
        }
        else if(maxts < startts) {
          
          dataloadedmetadata.put(DataLoadedMetadata.dimensionSetStartTime, maxts.toString)
          (maxts, Utility.getPreviousTimeForGranularity(startts, baseGran.getGranularity, Calendar.getInstance(TimeZone.getTimeZone(ConfConstants.timezone))))
        }
        else
          (0l, 0l)

      Utility.getEmptySchemaRDD(sqlContext, latestschema).registerTempTable(baseDimensionSetTable)
      
      var flag = false
      if(startTime != 0 && endTime != 0) { 
        
        val _$list = Utility.getAllInclusiveIntervals(startTime, endTime, baseGran.getGranularity)
        val _list = for(timestamp <- _$list) yield {
        val baseDir = instabase + "/" + instainstanceid + "/" + "bin-class" + "/" + "base-level" + "/" + baseCube.cubeName + "/d/" + timestamp
        val rowRDD = getRowSchemaRDD(sqlContext, baseDir, fields, datatypearray, latestschema)
        rowRDD
      }
      if(!_list.isEmpty)
        _list.reduce(_.unionAll(_)).registerTempTable(baseDimensionSetTable)
      }
      
      if(startTime != 0 && endTime != 0) { 
            
        DataLoader.putMetadata(acumeCache, dataloadedmetadata)
      }
    } catch { 
    case ex: Throwable => throw new IllegalStateException(ex)
    }
    true
  }
  
  private def max(xs: List[Long]): Option[Long] = xs match {
  case Nil => None
  case List(x: Long) => Some(x)
  case x :: y :: rest => max( (if (x > y) x else y) :: rest )
  } 
}



