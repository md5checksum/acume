package com.guavus.acume.cache.disk.utility

import java.util.Arrays
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.Breaks._

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.StructField
import org.apache.spark.sql.StructType
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.LongType

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.DimensionTable
import com.guavus.acume.cache.common.LevelTimestamp
import com.guavus.acume.cache.core.AcumeCache
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContext
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.acume.cache.workflow.AcumeCacheContextTrait
import com.guavus.insta.BinPersistTimeInfoRequest
import com.guavus.insta.BinPersistTimeInfoRequest
import com.guavus.insta.Insta
import com.guavus.insta.InstaCubeMetaInfo
import com.guavus.insta.InstaRequest

class InstaDataLoader(@transient acumeCacheContext: AcumeCacheContextTrait, @transient  conf: AcumeCacheConf, @transient acumeCache: AcumeCache) extends DataLoader(acumeCacheContext, conf, null) {

  @transient var insta: Insta = null
  @transient val sqlContext = acumeCacheContext.cacheSqlContext
  @transient var cubeList: List[InstaCubeMetaInfo] = null
  init
  
  def init() {
    insta = new Insta(acumeCacheContext.cacheSqlContext.sparkContext)
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
        businessCube.binsource, dimSet.cubeName, List(), measureFilters)
        print("Firing aggregate query on insta "  + instaMeasuresRequest)
      val aggregatedMeasureDataInsta = insta.getAggregatedData(instaMeasuresRequest)
      print(aggregatedMeasureDataInsta.count)
      val dtnm = dTableName.tblnm
      val aggregatedTbl = "aggregatedMeasureDataInsta" + levelTimestamp.level + "_" + levelTimestamp.timestamp
      sqlContext.registerRDDAsTable(aggregatedMeasureDataInsta, aggregatedTbl)
      

      val selectField = dtnm + ".id, " +  CubeUtil.getCubeFields(businessCube).map(aggregatedTbl+ "." + _).mkString(",")
      val onField = CubeUtil.getDimensionSet(businessCube).map(x => aggregatedTbl + "." + x.getName + "=" + dtnm + "." + x.getName).mkString(" AND ")
      val ar = sqlContext.sql(s"select $selectField from $aggregatedTbl left outer join $dtnm on $onField")
      val fullRdd = generateId(ar, dTableName)
      val joinedTbl = businessCube.cubeName + levelTimestamp.level + "_" + levelTimestamp.timestamp
      sqlContext.registerRDDAsTable(fullRdd, joinedTbl)

      (correctMTable(businessCube, joinedTbl, levelTimestamp.timestamp),
        correctDTable(businessCube, dTableName, joinedTbl, levelTimestamp.timestamp))
    }
  }
  
  def correctMTable(businessCube: Cube, joinedTbl: String, timestamp : Long) = {
    
    val measureSet = CubeUtil.getMeasureSet(businessCube).map(_.getName).mkString(",")
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
    val selectDimensionSet = cubeDimensionSet.map(_.getName).mkString(",")
    val newdrdd = sqlContext.sql(s"select id as tupleid, $selectDimensionSet from $joinedTbl")
    val dimensionRdd = newdrdd.union(table(dimensiontable.tblnm))
    dimensiontable.Modify
    sqlContext.registerRDDAsTable(sqlContext.applySchema(dimensionRdd, latestschema), dimensiontable.tblnm)
    dimensiontable.tblnm
  }
  
  def generateId(drdd: SchemaRDD, dtable : DimensionTable): SchemaRDD = {

    //    val size = drdd.partitions.size
    //    drdd.coalesce(size/10+1)

    /**
     * formula = (k*n*10+index*10+max+j+1,k*n*10+index*10+10+max+j+1)
     */

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
//    def func(partionIndex : Int, itr : Iterator[Row]) = 
	
    val returnRdd = sqlContext.applySchema(drdd.mapPartitionsWithIndex((partionIndex, itr) => {
      var k,j = 0
      val temp = itr.map(x => {
        if(x(0) == null) {
          val arr = new Array[Any](x.size)
          x.copyToArray(arr)
          if(j >= 10) {
            j=0
            k+=1
          }
          arr(0) = k* numPartitions*10 + partionIndex*10 + lastMax + 1 + j
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
    }), drdd.schema)
    dtable.maxid = acc.value.data.max
    returnRdd
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
        for (gran <- cube.aggregationIntervals.filter(_ != -1)) {
          if (startTime == Utility.floorFromGranularity(startTime, gran)) {
            var tempEndTime = Utility.getNextTimeFromGranularity(startTime, gran, Utility.newCalendar)
            while(tempEndTime < endTime) tempEndTime = Utility.getNextTimeFromGranularity(tempEndTime, gran, Utility.newCalendar)
            if(tempEndTime == endTime) {
            	isValidCubeGran = true
            	break
            }
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
  override def getFirstBinPersistedTime(binSource : String) : Long =  {
	  		val granToIntervalMap = getBinSourceToIntervalMap(binSource)
		  granToIntervalMap.get(-1).getOrElse(throw new IllegalArgumentException("No Data found in insta for default gran -1 and binSource :" + binSource))._1
  }
  
  override def getLastBinPersistedTime(binSource : String) : Long =  {
	  		val granToIntervalMap = getBinSourceToIntervalMap(binSource)
		   granToIntervalMap.get(-1).getOrElse(throw new IllegalArgumentException("No Data found in insta for default gran -1 and binSource :" + binSource))._2
  }
  
  override def getBinSourceToIntervalMap(binSource : String) : Map[Long, (Long,Long)] =  {
		  getAllBinSourceToIntervalMap.getOrElse(binSource, throw new IllegalArgumentException("No Data found for binSource " +  binSource))
  }
  
  override def getAllBinSourceToIntervalMap() : Map[String,Map[Long, (Long,Long)]] =  {
    val persistTime = insta.getAllBinPersistedTimes
    print(persistTime)
    persistTime.map(binSourceToGranToAvailability => {
      val minGran = binSourceToGranToAvailability._2.filter(_._1 != -1).keys.min
      val granularityToAvailability = binSourceToGranToAvailability._2.map(granToAvailability => {
        if (granToAvailability._1 == -1) {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, minGran, Utility.newCalendar)))
        } else {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, granToAvailability._1, Utility.newCalendar)))
        }
      })
      (binSourceToGranToAvailability._1,  granularityToAvailability ++ Map(-1L -> granularityToAvailability.get(minGran).get) 
      )
    })
  }
}

case class Vector(val data: Array[Long]) extends Serializable{}
