package com.guavus.acume.cache.workflow

import java.io.FileInputStream
import java.util.Random

import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.AcumeConstants
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.common.Cube
import com.guavus.acume.cache.common.CubeMeasure
import com.guavus.acume.cache.common.CubeMeasureSet
import com.guavus.acume.cache.common.DataType
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.DimensionSet
import com.guavus.acume.cache.common.FieldType
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.MeasureSet
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.common.QLType.QLType
import com.guavus.acume.cache.core.AcumeCacheFactory
import com.guavus.acume.cache.core.CacheIdentifier
import com.guavus.acume.cache.core.TimeGranularity
import com.guavus.acume.cache.gen.Acume
import com.guavus.acume.cache.utility.SQLUtility
import com.guavus.acume.cache.utility.Utility

import javax.xml.bind.JAXBContext

class AcumeCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) extends Serializable { 
  sqlContext match{
  case hiveContext: HiveContext =>
  case sqlContext: SQLContext => 
  case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
 
  @transient
  val rrCacheLoader = Class.forName(conf.get(ConfConstants.rrloader)).getConstructors()(0).newInstance(this, conf).asInstanceOf[RRCache]
  private [cache] val dimensionMap = new HashMap[String, Dimension]
  private [cache] val measureMap = new HashMap[String, Measure]
  private [cache] val vrmap = HashMap[Long, Int]()
  private [cache] val cubeMap = HashMap[String, Cube]()
  private [cache] val cubeList = MutableList[Cube]()
  //todo how will this be done
  private [cache] val baseCubeMap = HashMap[String, BaseCube]()
  private [cache] val baseCubeList = MutableList[BaseCube]()
  private [acume] def getCubeList = cubeList.toList
  private [acume] def isDimension(name: String) = 
    if(dimensionMap.contains(name)) true 
    else if(measureMap.contains(name)) false 
    else throw new RuntimeException("Field nither in Dimension Map nor in Measure Map.")

  loadXML(conf.get(ConfConstants.businesscubexml))
  loadVRMap(conf)
  loadXMLCube("")
  
  private def getCubeName(tableName: String) = tableName.substring(0, tableName.indexOf(AcumeConstants.TRIPLE_DOLLAR_SSC) + 1)
  private [acume] def utilQL(sql: String, qltype: QLType) = {
    val tx = AcumeCacheContext.parseSql(sql)
    val rt = tx._2
    var newsql = sql
    var i = ""
    val list = for(l <- tx._1) yield {
      val cube = l.getTableName
      val startTime = l.getStartTime
      val endTime = l.getEndTime
      
      i = getTable(cube)
      val id = getCube(cube)
      newsql = newsql.replace(s" $cube ", s" $i ")
      val idd = new CacheIdentifier()
      idd.put("cube", id.hashCode)
      val instance = AcumeCacheFactory.getInstance(this, conf, idd, id)
      instance.createTempTableAndMetadata(startTime, endTime, rt, i,None)
    }
    val klist = list.flatMap(_.timestamps).toList
    val kfg = AcumeCacheContext.ACQL(qltype, sqlContext)(newsql)
    kfg.collect.foreach(println)
    AcumeCacheResponse(AcumeCacheContext.ACQL(qltype, sqlContext)(s"select * from $i"), MetaData(klist))
  }
  
  def acql(sql: String, qltype: String): AcumeCacheResponse = { 
    
    val ql = QLType.getQLType(qltype)
    if (!AcumeCacheContext.checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"ql not supported with ${sqlContext}");
    executeQl(sql, ql)
  }
  
  def acql(sql: String): AcumeCacheResponse = { 
    
    val ql = AcumeCacheContext.getQLType(conf)
    if (!AcumeCacheContext.checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"ql not supported with ${sqlContext}");
    executeQl(sql, ql)
  }
  
  def executeQl(sql : String, ql : QLType.QLType) = {
    rrCacheLoader.getRdd((sql, ql))
  }
  
  private [acume] def getFieldsForCube(name: String) = {
      
    val cube = cubeMap.getOrElse(name, throw new RuntimeException(s"Cube $name Not in AcumeCache knowledge."))
    cube.dimension.dimensionSet.map(_.getName) ++ cube.measure.measureSet.map(_.measure.getName)
  }
  
  private [acume] def getDefaultAggregateFunction(stringname: String) = {
    val measure = measureMap.getOrElse(stringname, throw new RuntimeException(s"Measure $stringname not in Acume knowledge."))
    measure.getDefaultAggregationFunction
  }
  
  private [acume] def getDefaultValue(fieldName: String) = {
    if(isDimension(fieldName))
      dimensionMap.get(fieldName).get.getDefaultValue
    else
      measureMap.get(fieldName).get.getDefaultValue
  }
  
  private [acume] def getCubeListContainingFields(lstfieldNames: List[String]) = {
    
    val dimensionSet = scala.collection.mutable.Set[Dimension]()
    val measureSet = scala.collection.mutable.Set[Measure]()
    for(field <- lstfieldNames)
      if(isDimension(field))
        dimensionSet.+=(dimensionMap.get(field).get)
      else
        measureSet.+=(measureMap.get(field).get)
      val kCube = 
        for(cube <- cubeList if(dimensionSet.subsetOf(cube.dimension.dimensionSet) && measureSet.subsetOf(cube.measure.measureSet.map(_.measure)))) yield {
          cube
        }
    kCube.toList
  }
  
  private [cache] def getCube(cube: String) = cubeMap.get(cube).getOrElse(throw new RuntimeException(s"cube $cube not found."))
  
  private [cache] def getTable(cube: String) = cube + "_" + getUniqueRandomNo 	
  
  private [cache] def getUniqueRandomNo: String = System.currentTimeMillis() + "" + Math.abs(new Random().nextInt())
  
  private [cache] def loadVRMap(conf: AcumeCacheConf) = {
    val vrmapstring = conf.get(ConfConstants.variableretentionmap)
    vrmap.++=(Utility.getLevelPointMap(vrmapstring))
  }
    
  private [workflow] def loadBaseXML(filedir: String) = {
    
    
  }
  
  private [workflow] def loadXMLCube(xml: String) = {
    
    baseCubeList.++=(cubeList.map(x => BaseCube(x.cubeName, x.dimension, MeasureSet(x.measure.measureSet.map(y => y.measure)))))
    baseCubeMap.++=(cubeMap.map(x => (x._1, BaseCube(x._2.cubeName, x._2.dimension, MeasureSet(x._2.measure.measureSet.map(y => y.measure))))))
  }
  
  private [workflow] def loadXML(xml: String) = { 
    
    val jc = JAXBContext.newInstance("com.guavus.acume.cache.gen")
    val unmarsh = jc.createUnmarshaller()
    val acumeCube = unmarsh.unmarshal(new FileInputStream(xml)).asInstanceOf[Acume]
    for(lx <- acumeCube.getFields().getField().toList) { 

      val info = lx.getInfo.split(':')
      val name = info(0).trim
      val datatype = DataType.getDataType(info(1).trim)
      val fitype = FieldType.getFieldType(info(2).trim)
      val functionName = if(info.length<4) "none" else info(3) 	
      fitype match{
        case FieldType.Dimension => 
          dimensionMap.put(name.trim, new Dimension(name, datatype, 0))
        case FieldType.Measure => 
          measureMap.put(name.trim, new Measure(name, datatype, functionName, 0 ))
      }
    }
    
    val defaultPropertyTuple = acumeCube.getDefault.split(",").map(_.trim).map(kX => {
          val xtoken = kX.split(":")
          (xtoken(0).trim, xtoken(1).trim)
        })
        
    val defaultPropertyMap = defaultPropertyTuple.toMap
    
    val list = 
      for(c <- acumeCube.getCubes().getCube().toList) yield {
        val cubeName = c.getName().trim
        val fields = c.getFields().split(",").map(_.trim)
        val dimensionSet = scala.collection.mutable.Set[Dimension]()
        val measureSet = scala.collection.mutable.Set[CubeMeasure]()
        for(ex <- fields){
          val array = ex.split(":")
          val fieldName = array(0).trim
          val function = if(array.length<2) "" else array(1) 	

          //only basic functions are supported as of now. 
          //Extend this to support custom udf of hive as well.
          if(!function.isEmpty()){
            if(isDimension(fieldName))
                throw new RuntimeException("Aggregation functions are not supported on Dimension.")
          }
          dimensionMap.get(fieldName) match{
          case Some(dimension) => 
            dimensionSet.+=(dimension)
          case None =>
            measureMap.get(fieldName) match{
            case None => throw new Exception("Field not registered.")
            case Some(measure) => measureSet.+=(CubeMeasure(measure, function))
            }
          }
        }
        
        val _$cubeProperties = c.getProperties()
        val _$propertyMap = _$cubeProperties.split(",").map(x => {
          val i = x.indexOf(":")
          (x.substring(0, i).trim, x.substring(i+1, x.length).trim)
        })
        val propertyMap = _$propertyMap.toMap
        
        val levelpolicymap = Utility.getLevelPointMap(getProperty(propertyMap, defaultPropertyMap, ConfConstants.levelpolicymap, cubeName))
        val timeserieslevelpolicymap = Utility.getLevelPointMap(getProperty(propertyMap, defaultPropertyMap, ConfConstants.timeserieslevelpolicymap, cubeName))
        val Gnx = getProperty(propertyMap, defaultPropertyMap, ConfConstants.basegranularity, cubeName)
        val granularity = TimeGranularity.getTimeGranularityForVariableRetentionName(Gnx).getOrElse(throw new RuntimeException("Granularity doesnot exist " + Gnx))
        val cube = Cube(cubeName, DimensionSet(dimensionSet.toSet), CubeMeasureSet(measureSet.toSet), granularity, true, levelpolicymap, timeserieslevelpolicymap)
        cubeMap.put(cubeName, cube)
        cube
      }
    cubeList.++=(list)
  }
  
  private def getProperty(propertyMap: Map[String, String], defaultPropertyMap: Map[String, String], name: String, nmCube: String) = {
    
    propertyMap.getOrElse(name, defaultPropertyMap.getOrElse(name, throw new RuntimeException(s"The configurtion $name should be done for cube $nmCube")))
  }
  }

object AcumeCacheContext{
  
  def main(args: Array[String]) { 
    
    val sqlContext = new SQLContext(new SparkContext)
    val conf = new AcumeCacheConf
    conf.set(ConfConstants.businesscubexml, "/Users/archit.thakur/Documents/Code_Acume_Scala/cache/src/test/resources/cubdefinition.xml")
    conf.set("acume.cache.core.variableretentionmap", "1h:720")
    conf.set("acume.cache.baselayer.instainstanceid","0")
    conf.set("acume.cache.baselayer.storagetype", "orc")
    conf.set("acume.cache.core.timezone", "GMT")
    conf.set("acume.cache.baselayer.instabase","instabase")
    conf.set("acume.cache.baselayer.cubedefinitionxml", "cubexml")
    conf.set("acume.cache.execute.qltype", "sql")
    val cntxt = new AcumeCacheContext(sqlContext, conf)
    cntxt.acql("select * from searchEgressPeerCube_12345")
  }
  
  private [workflow] def parseSql(sql: String) = { 
    
    val util = new SQLUtility();
    val list = util.getList(sql);
    val requestType = util.getRequestType(sql);
    (list, RequestType.getRequestType(requestType))
  }
  
  private [cache] def checkQLValidation(sqlContext: SQLContext, qltype: QLType) = { 
    
    sqlContext match{
      case hiveContext: HiveContext =>
        qltype match{
          case QLType.hql | QLType.sql => true
          case rest => false
        }
      case sqlContext: SQLContext => 
        qltype match{
          case QLType.sql => true
          case rest => false
        }
    }
  }
  
  private def getQLType(conf: AcumeCacheConf) = QLType.getQLType(conf.get(ConfConstants.qltype)) 	
  
  private[cache] def ACQL(qltype: QLType, sqlContext: SQLContext) = { 
    
    if(sqlContext.isInstanceOf[HiveContext]){
    
      qltype match{
      case QLType.hql => sqlContext.asInstanceOf[HiveContext].hql(_)
      case QLType.sql => sqlContext.sql(_)
      }
    }
    else if(sqlContext.isInstanceOf[SQLContext]) { 
      
      qltype match{
      case QLType.sql => sqlContext.sql(_)
      }
    }
    sqlContext.sql(_)
  }
}

