package com.guavus.acume.cache.disk.utility

import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.LevelTimestamp
import org.apache.spark.sql.SchemaRDD
import com.guavus.acume.cache.common.DimensionTable
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.Utility
import com.guavus.insta.Insta
import com.guavus.insta.InstaCubeMetaInfo
import com.guavus.insta.InstaRequest
import scala.util.control.Breaks._
import java.util.Arrays
import java.util.concurrent.ConcurrentHashMap
import com.guavus.insta.BinPersistTimeInfoRequest
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import org.apache.spark.sql.StructField
import org.apache.spark.sql.StructType
import com.guavus.acume.cache.common.ConversionToSpark
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.AccumulatorParam

class InstaDataLoader(acumeCacheContext: AcumeCacheContextTrait, conf: AcumeCacheConf, acumeCache: AcumeCache) extends DataLoader(acumeCacheContext, conf, null) {

  var insta: Insta = null
  val sqlContext = acumeCacheContext.cacheSqlContext
  var cubeList: List[InstaCubeMetaInfo] = null
  init
  
  def init() {
    val insta = new Insta(acumeCacheContext.cacheSqlContext.sparkContext)
//    insta.init(conf.get(ConfConstants.backendDbName, throw new IllegalArgumentException(" Insta DBname is necessary for loading data from insta")), conf.get(ConfConstants.cubedefinitionxml, throw new IllegalArgumentException(" Insta cubeDefinitionxml is necessary for loading data from insta")))
    insta.init(conf.get(ConfConstants.backendDbName), conf.get(ConfConstants.cubedefinitionxml))
    cubeList = insta.getInstaCubeList
  }

  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable): Tuple2[SchemaRDD, String] = {
    this.synchronized {
      val endTime = Utility.getNextTimeFromGranularity(levelTimestamp.timestamp, levelTimestamp.level.localId, Utility.newCalendar)
      val dimSet = getBestCubeName(businessCube, levelTimestamp.timestamp, endTime)
      val fields = CubeUtil.getCubeFields(businessCube)
//      val dimensionFilters = (dimSet.dimensions).map(x => {
//        if (fields.contains(x))
//          1
//        else
//          0
//      }) ++ dimSet.measures.map(x => 0)

      val measureFilters = (dimSet.dimensions ++ dimSet.measures).map(x => {
        if (fields.contains(x))
          1
        else
          0
      })

      val instaMeasuresRequest = InstaRequest(levelTimestamp.timestamp, endTime,
        businessCube.binsource, dimSet.cubeName, null, measureFilters)

      val aggregatedMeasureDataInsta = insta.getAggregatedData(instaMeasuresRequest)
      val dtnm = dTableName.tblnm

      val selectField = CubeUtil.getCubeFields(businessCube).map("aggregatedMeasureDataInsta." + _).mkString(",") + ", dtnm.id"
      val onField = CubeUtil.getDimensionSet(businessCube).map(x => "aggregatedMeasureDataInsta." + x + "=dtnm." + x).mkString(" AND ")
      val ar = sqlContext.sql(s"select $selectField from aggregatedMeasureDataInsta left outer join dtnm on $onField")
      val fullRdd = generateId(ar, dTableName)
      val joinedTbl = businessCube.toString + levelTimestamp.level + "_" + levelTimestamp.timestamp
      fullRdd.registerTempTable(joinedTbl)

      (correctMTable(businessCube, joinedTbl, levelTimestamp.timestamp),
        correctDTable(businessCube, dTableName, joinedTbl, levelTimestamp.timestamp))
    }
  }
  
  def correctMTable(businessCube: Cube, joinedTbl: String, timestamp : Long) = {
    
    val measureSet = CubeUtil.getMeasureSet(businessCube).mkString(",")
    sqlContext.sql(s"select id as tupleid, $timestamp as ts, $measureSet from $joinedTbl")
  }
  
  def correctDTable(cube: Cube, dimensiontable: DimensionTable, joinedTbl: String, timestamp: Long): String = {
    
    val cubeDimensionSet = CubeUtil.getDimensionSet(cube)
    val schema = 
        cubeDimensionSet.map(field => { 
          StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
        })
          
    import sqlContext._
    val latestschema = StructType(StructField("id", LongType, true) +: schema.toList)
    
    val newdrdd = sqlContext.sql(s"select id as tupleid, $timestamp as ts, $cubeDimensionSet from $joinedTbl")
    dimensiontable.Modify
    sqlContext.applySchema(newdrdd.union(table(dimensiontable.tblnm)), latestschema).registerTempTable(dimensiontable.tblnm)
    dimensiontable.tblnm
  }
  
  def generateId(drdd: SchemaRDD, dtable : DimensionTable): SchemaRDD = {

    //    val size = drdd.partitions.size
    //    drdd.coalesce(size/10+1)

    /**
     * formula = (k*n*10+index*10+max+j+1,k*n*10+index*10+10+max+j+1)
     */

    case class Vector(val data: Array[Long]) {}
    implicit object VectorAP extends AccumulatorParam[Vector] {
      def zero(v: Vector) = new Vector(new Array(v.data.size))
      def addInPlace(v1: Vector, v2: Vector) = {
        for (i <- 0 to v1.data.size - 1) 
        	v1.data(i) += v2.data(i)
        v1
      }
    }
    
	val numPartitions = drdd.partitions.size
    val accumulatorList = new Array[Long](numPartitions)
    Arrays.fill(accumulatorList, 0)
    val acc = sqlContext.sparkContext.accumulator(new Vector(accumulatorList))
    val lastMax = dtable.maxid
    val idIndex = 0 
    def func(partionIndex : Int, itr : Iterator[Row]) = {
      var k,j = 0
      val temp = itr.map(x => {
        if(x(idIndex) == null) {
          val arr = new Array[Any](x.size)
          x.copyToArray(arr)
          if(j >= 10) {
            j=0
            k+=1
          }
          arr(idIndex) = k* numPartitions*10 + partionIndex*10 + lastMax + 1 + j
          j+=1
          Row.fromSeq(arr)
        } else 
        	x
      })
      val arr = new Array[Long](numPartitions)
      Arrays.fill(arr, 0)
      arr(partionIndex) = k* numPartitions*10 + partionIndex*10 + lastMax + 1 + j 
      acc += new Vector(arr) 
      temp
    }
	dtable.maxid = acc.value.data.max
    sqlContext.applySchema(drdd.mapPartitionsWithIndex(func), drdd.schema)
//    drdd.mapPartitions(f, preservesPartitioning)
  }
  
  def getId(max: Long, index: Long, totalLength: Long) = {
    
    
  }

  def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, dTableName: DimensionTable, instabase: String, instainstanceid: String): Tuple2[SchemaRDD, String] = {
    loadData(businessCube, levelTimestamp, dTableName)
  }

  def getBestCubeName(businessCube: Cube, startTime: Long, endTime: Long): InstaCubeMetaInfo = {
    val fieldnum = Integer.MAX_VALUE;
    var resultantTable = null;
    var bestCube: InstaCubeMetaInfo = null
    for (cube <- cubeList) {
      var isValidCubeGran = false
      breakable {
        for (gran <- cube.aggregationIntervals) {
          if (startTime == Utility.floorFromGranularity(startTime, gran) && endTime == Utility.getNextTimeFromGranularity(startTime, gran, Utility.newCalendar)) {
            isValidCubeGran = true
            break
          }
        }
      }
      if (isValidCubeGran && cube.binSource == businessCube.binsource && CubeUtil.getCubeFields(businessCube).toSet.subsetOf((cube.dimensions ++ cube.measures).toSet)) {
        if (bestCube == null) {
          bestCube = cube
        } else {
          if (bestCube.dimensions.size > cube.dimensions.size) {
            bestCube = cube
          } else if (bestCube.dimensions.size == cube.dimensions.size) {
            if (bestCube.measures.size > cube.measures.size) {
              bestCube = cube
            }
          }
        }
      }
    }
    if (bestCube == null) {
      throw new IllegalArgumentException("Could not find any cube for fields " + CubeUtil.getCubeFields(businessCube))
    }
    bestCube
  }

  private val metadataMap = new ConcurrentHashMap[Cube, DataLoadedMetadata]

  private[cache] def getMetadata(key: Cube) = metadataMap.get(key)
  private[cache] def putMetadata(key: Cube, value: DataLoadedMetadata) = metadataMap.put(key, value)
  private[cache] def getOrElseInsert(key: Cube, defaultValue: DataLoadedMetadata): DataLoadedMetadata = {

    if (getMetadata(key) == null) {

      putMetadata(key, defaultValue)
      defaultValue
    } else
      getMetadata(key)
  }

  private[cache] def getOrElseMetadata(key: Cube, defaultValue: DataLoadedMetadata): DataLoadedMetadata = {

    if (getMetadata(key) == null)
      defaultValue
    else
      getMetadata(key)
  }

  def getDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, acumeCache: AcumeCache) = {

    val dataLoaderClass = StorageType.getStorageType(conf.get(ConfConstants.storagetype)).dataClass
    val loadedClass = Class.forName(dataLoaderClass)
    val newInstance = loadedClass.getConstructor(classOf[AcumeCacheContext], classOf[AcumeCacheConf], classOf[AcumeCache]).newInstance(acumeCacheContext, conf, acumeCache)
    newInstance.asInstanceOf[DataLoader]
  }

}