//package com.guavus.acume.cache.disk.utility
//
//import java.util.Random
//import scala.Array.canBuildFrom
//import scala.Array.fallbackCanBuildFrom
//import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
//import org.apache.hadoop.hive.ql.io.orc.OrcStruct
//import org.apache.hadoop.io.NullWritable
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.StructField
//import org.apache.spark.sql.StructType
//import org.apache.spark.sql.catalyst.expressions.Row
//import com.guavus.acume.cache.common.AcumeCacheConf
//import com.guavus.acume.cache.common.CacheLevel.CacheLevel
//import com.guavus.acume.cache.common.ConfConstants
//import com.guavus.acume.cache.common.ConversionToSpark
//import com.guavus.acume.cache.common.Cube
//import com.guavus.acume.cache.common.LevelTimestamp
//import com.guavus.acume.cache.workflow.AcumeCacheContext
//import org.apache.spark.sql.catalyst.types.LongType
//import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
//
//class ORCDataLoader(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf, cube: Cube) extends DataLoader(acumeCacheContext, conf, cube) { 
//  
//  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, DTableName: String) = { 
//    
//    val instabase = conf.get(ConfConstants.instabase)
//    val instainstanceid = conf.get(ConfConstants.instainstanceid)
//    loadData(businessCube, levelTimestamp, DTableName, instabase, instainstanceid)
//  }
//  
//  override def loadData(businessCube: Cube, levelTimestamp: LevelTimestamp, DTableName: String, instabase: String, instainstanceid: String) = { 
//    
//    val level = levelTimestamp.level
//    val list = getLevel(levelTimestamp) //list of timestamps to be loaded on base gran, improve this to support grans in insta . 	
//    val baseCube = CubeUtil.getCubeMap(acumeCacheContext.baseCubeList.toList, acumeCacheContext.cubeList.toList).getOrElse(businessCube, throw new RuntimeException("Value not found."))
//    val thisCubeName = baseCube.cubeName + getUniqueRandomeNo
//    val sparkContext = acumeCacheContext.sqlContext.sparkContext
//    var flag = false
//
//    val schema = CubeUtil.getMeasureSet(baseCube).map(field => { 
//            StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
//          })
//    val latestschema = StructType(StructField("tupleid", LongType, true) +: StructField("ts", LongType, true) +: schema.toList)
//          
//    for(ts <- list) {
//    
//      val baseDir = instabase + "/" + instainstanceid + "/" + "bin-class" + "/" + "base-level" + "/" + baseCube.cubeName + "/f/" + ts
//      val rowRDD = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](baseDir).map(getRow)
//      
//      val schemaRDD = acumeCacheContext.sqlContext.applySchema(rowRDD, latestschema)
//
//      if(!flag) {
//        schemaRDD.registerTempTable(thisCubeName)
//        flag = true
//      } else
//        schemaRDD.insertInto(thisCubeName)
//    }
//    
//    joinDimensionSet(businessCube, level, list.toList, DTableName, thisCubeName, instabase, instainstanceid)
//    
//    //explore hive udfs for aggregation.
//    
////    val aggregatedRDD = sqlContext.sql("select " + baseCubeDimensionList + ", " + baseCubeAggregatedMeasureList + " from " + thisCubeName + " groupBy " + baseCubeDimensionList)
//    //check if there is a better way to compute aggregatedRDD.
////    val annotatedRDD = aggregatedRDD.map(x => { 
////      new WritableTuple(x.toArray) 
////      })
//    
//    //remove dependency from crux. write things at acume level. 	
//    
////    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
//    
//  }
//  
//  private def getLevel(levelTimestamp: LevelTimestamp) = CubeUtil.getLevel(levelTimestamp)
//  
//  def loadDimensionSet(businessCube: Cube, list: List[Long], instabase: String, instainstanceid: String, globalDTableName: String): Boolean = { 
//    
//    // This loads the dimension set of cube businessCubeName for the particular timestamp into globalDTableName table.
//    try { 
//      val sqlContext = acumeCacheContext.sqlContext
//      val sparkContext = sqlContext.sparkContext
//      
//      val baseCube = CubeUtil.getCubeMap(acumeCacheContext.baseCubeList.toList, acumeCacheContext.cubeList.toList).getOrElse(businessCube, throw new RuntimeException("Value not found."))
//      val thisCubeName = baseCube.cubeName + getUniqueRandomeNo
//      
//      val schema = 
//        CubeUtil.getDimensionSet(baseCube).map(field => { 
//          StructField(field.getName, ConversionToSpark.convertToSparkDataType(CubeUtil.getFieldType(field)), true)
//        })
//          
//      val latestschema = StructType(StructField("id", LongType, true) +: StructField("ts", LongType, true) +: schema.toList)
//          
//      var flag = false
//      for(timestamp <- list){
//        val baseDir = instabase + "/" + instainstanceid + "/" + "bin-class" + "/" + "base-level" + "/" + baseCube.cubeName + "/d/" + timestamp
//        val rowRDD = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat](baseDir).map(getRow)
//        val schemaRDD = sqlContext.applySchema(rowRDD, latestschema)
//      
//        if(!flag) { 
//      
//          schemaRDD.registerTempTable(thisCubeName)
//          flag = true
//        } else
//          schemaRDD.insertInto(thisCubeName)
//      }
//      val getdimension = CubeUtil.getDimensionSet(businessCube).map(_.getName).mkString(",")
//      val dimensionSQL = s"select id, ts, ${getdimension} from $thisCubeName"
//      import acumeCacheContext.sqlContext._
//      val istableregistered = 
//        try{
//        table(globalDTableName)
//        true
//        } catch{
//        case ex: Exception => false
//        }
//        val dimensionRDD = sqlContext.sql(dimensionSQL)
//        sqlContext.applySchema(dimensionRDD, dimensionRDD.schema)
//        if(istableregistered) 
//          dimensionRDD.insertInto(globalDTableName)
//        else 
//          dimensionRDD.registerTempTable(globalDTableName)
//      true
//    } catch { 
//    case ex: Throwable => false   
//    }
//  }
//  
//  def getUniqueRandomeNo: String = {
//    val num = System.currentTimeMillis() + "" + Math.abs(new Random().nextInt)
//    num
//  }
//  
//  def getRow(tuple: (NullWritable, OrcStruct)) = {
//  
//    val struct = tuple._2
//    val field = struct.toString.substring(1)
//    val l = field.length
//    val tokenList = field.substring(0, field.length - 2).split(',').map(_.trim)
//    Row(tokenList)
//  }
//  
//  def joinDimensionSet(businessCube: Cube, level: CacheLevel, list: List[Long], globalDTableName: String, thisCubeName: String, instabase: String, instainstanceid: String) = { 
//    
//    val sqlContext = acumeCacheContext.sqlContext
//    val businessCubeAggregatedMeasureList = CubeUtil.getStringMeasureOrFunction(acumeCacheContext.measureMap.toMap, cube)
//    val local_thisCubeName = thisCubeName + getUniqueRandomeNo
//    loadDimensionSet(businessCube, list, instabase, instainstanceid, globalDTableName)
//    val str = "select tupleid, " + businessCubeAggregatedMeasureList + " from " + thisCubeName + " group by tupleid"
//    val join = s"Select * from $globalDTableName INNER JOIN $local_thisCubeName ON $globalDTableName.id = $local_thisCubeName.tupleid"
//    val aggregatedRDD = sqlContext.sql(str)
//    sqlContext.applySchema(aggregatedRDD, aggregatedRDD.schema).registerTempTable(local_thisCubeName)
//    sqlContext.sql(join)
//    //explore hive udfs for aggregation.
//    //remove dependency from crux. write things at acume level. 	
//    
////    val stream  = new Transform("Transform", new Stream(new StreamMetaData("inname", "junk", new Fields((baseCubeDimensionList++baseCubeAggregatedMeasureAliasList).toArray)), annotatedRDD).streamMetaData, new StreamMetaData("outname","junk",new Fields), List(new CopyAnnotation(new Fields(), new Fields()))).operate
//   
//    
//    //select tupleid, SUM(XPacific_B) as XPacific_B,SUM(Local_B) as Local_B,timestamp,SUM(XOthers_B) as XOthers_B,SUM(On_net_B) as On_net_B,SUM(XAtlantic_B) as XAtlantic_B,SUM(Off_net_B) as Off_net_B,SUM(TTS_B) as TTS_B,SUM(Regional_B) as Regional_B,SUM(Continental_B) as Continental_B from searchEgressPeerCube1413440614626738536130 groupBy tupleid
//  }
//}
//
//object ORCDataLoader{
//  
//  def main(args: Array[String]) { 
//    
//    val conf = new SparkConf
//    conf.set("spark.master", "local")
//    conf.set("spark.app.name", "local")
//    val sqlContext = new SQLContext(new SparkContext(conf))
//    val conf123 = new AcumeCacheConf
//    conf123.set(ConfConstants.businesscubexml, "/Users/archit.thakur/Documents/Code_Acume_Scala/cache/src/test/resources/cubdefinition.xml")
//    conf123.set("acume.cache.core.variableretentionmap", "1h:720")
//    conf123.set("acume.cache.baselayer.instainstanceid","0")
//    conf123.set("acume.cache.baselayer.storagetype", "orc")
//    conf123.set("acume.cache.core.timezone", "GMT")
//    conf123.set("acume.cache.baselayer.instabase","instabase")
//    conf123.set("acume.cache.baselayer.cubedefinitionxml", "cubexml")
//    conf123.set("acume.cache.execute.qltype", "sql")
//    val cntxt = new com.guavus.acume.cache.workflow.AcumeCacheContext(sqlContext, conf123)
//    cntxt.acql("select * from searchEgressPeerCube_12345")
//  }
//}
//
//
//
//
