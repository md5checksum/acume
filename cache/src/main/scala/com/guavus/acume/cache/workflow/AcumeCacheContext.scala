package com.guavus.acume.cache.workflow

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import java.lang.UnsupportedOperationException
import com.guavus.acume.cache.common.QLType
import com.guavus.acume.cache.common.QLType._
import com.guavus.acume.cache.utility.SQLParserFactory
import com.guavus.acume.cache.utility.SQLTableGetter
import com.guavus.acume.cache.core.AcumeCacheFactory
import java.io.StringReader
import net.sf.jsqlparser.statement.select.Select
import scala.collection.JavaConversions._
import javax.xml.bind.JAXBContext
import com.guavus.acume.cache.gen.Acume
import java.io.FileInputStream
import com.guavus.acume.cache.common.FieldType
import com.guavus.acume.cache.common.DataType
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.common._
import com.guavus.acume.cache.core.AcumeCacheType
import com.guavus.acume.cache.core.TimeGranularity._
import com.guavus.acume.cache.core.TimeGranularity
import scala.collection.mutable.MutableList
import com.guavus.rubix.query.data.MeasureMapper
import com.guavus.acume.cache.util.Utility12345

class AcumeCacheContext(val sqlContext: SQLContext, val conf: AcumeCacheConf) { 
  sqlContext match{
  case hiveContext: HiveContext =>
  case sqlContext: SQLContext => 
  case rest => throw new RuntimeException("This type of SQLContext is not supported.")
  }
 
  AcumeCacheContext.loadXML(conf.get(ConfConstants.businesscubexml))
  AcumeCacheContext.loadVRMap(conf)
  
  def acql(sql: String, qltype: String) = { 
    
    val ql = QLType.getQLType(qltype)
    if(!AcumeCacheContext.checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"$ql not supported with $sqlContext")
    
    val parsedSQL = AcumeCacheContext.parseSql(sql)
    val tableList = parsedSQL._1
    val (startTime, endTime) = parsedSQL._2
    val tblCbeMap = tableList.map(string => (string, string.substring(0, string.indexOf("_")+1))).toMap
//    val systemloader = AcumeCacheFactory.getAcumeCache(name, conf.get(ConfConstants.whichcachetouse))
    val cacheLoader = AcumeCacheFactory.getInstance(this, conf, cacheIdentifier, cube)
  }
  
  def acql(sql: String) = { 
    
    val ql = AcumeCacheContext.getQLType(conf)
    if(!AcumeCacheContext.checkQLValidation(sqlContext, ql))
      throw new RuntimeException(s"$ql not supported with $sqlContext")
    val parsedSQL = AcumeCacheContext.parseSql(sql)
    val tableList = parsedSQL._1
    val (startTime, endTime) = parsedSQL._2
    val tblCbeMap = tableList.map(string => (string, string.substring(0, string.indexOf("_")+1))).toMap
//    val systemloader = AcumeCacheFactory.getAcumeCache(name, conf.get(ConfConstants.whichcachetouse))
  }
}

object AcumeCacheContext{
  
  private [cache] val dimensionMap = new HashMap[String, Dimension]
  private [cache] val measureMap = new HashMap[String, Measure]
  private [cache] val vrmap = HashMap[Long, Int]()
  private [cache] val cubeMap = HashMap[String, Cube]()
  private [cache] val cubeList = MutableList[Cube]()
  //todo how will this be done
  private [cache] val baseCubeMap = HashMap[String, BaseCube]()
  private [cache] val baseCubeList = MutableList[BaseCube]()
  
  private [acume] def getCubeList = cubeList
  private [acume] def isDimension(name: String) = 
    if(dimensionMap.contains(name)) true 
    else if(measureMap.contains(name)) false 
    else throw new RuntimeException("Field nither in Dimension Map nor in Measure Map.")
  private [acume] def getFieldsForCube(name: String) = {
      
    val cube = cubeMap.getOrElse(name, throw new RuntimeException(s"Cube $name Not in AcumeCache knowledge."))
    cube.dimension.dimensionSet.map(_.getName) ++ cube.measure.measureSet.map(_.getName)
  }
  
  private [acume] def getDefaultAggregateFunction(stringname: String) = {
    val measure = measureMap.getOrElse(stringname, throw new RuntimeException(s"Measure $stringname not in Acume knowledge."))
    measure.getFunction.functionName
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
        for(cube <- cubeList if(dimensionSet.subsetOf(cube.dimension.dimensionSet) && measureSet.subsetOf(cube.measure.measureSet))) yield {
          cube
        }
    kCube.toList
  }
  
  private [cache] def loadVRMap(conf: AcumeCacheConf) = {
    val vrmapstring = conf.get(ConfConstants.variableretentionmap)
    vrmap.++=(Utility12345.getLevelPointMap(vrmapstring))
  }
    
  private [workflow] def loadBaseXML(filedir: String) = {
    
    
  }
  
  private [workflow] def loadXML(xml: String) = { 
    
    val jc = JAXBContext.newInstance("com.guavus.acume.cache.gen")
    val unmarsh = jc.createUnmarshaller()
    val acumeCube = unmarsh.unmarshal(new FileInputStream(xml)).asInstanceOf[Acume]
    for(lx <- acumeCube.getFields().getField().toList) { 

      val info = lx.getInfo.split(':')
      val name = info(0)
      val datatype = DataType.getDataType(info(1))
      val fitype = FieldType.getFieldType(info(2))
      fitype match{
        case FieldType.Dimension => 
          dimensionMap.put(name, new Dimension(name, datatype))
        case FieldType.Measure => 
          measureMap.put(name, new Measure(name, datatype, Function("", info(3))))
      }
    }
    
    val defaultPropertyTuple = acumeCube.getDefault.split(",").map(_.trim).map(kX => {
          val xtoken = kX.split(":")
          (xtoken(0).trim, xtoken(1).trim)
        })
        
    val defaultPropertyMap = defaultPropertyTuple.toMap
    
    val list = 
      for(c <- acumeCube.getCubes().getCube().toList) yield {
        val cubeName = c.getName()
        val fields = c.getFields().split(",").map(_.trim)
        val dimensionSet = scala.collection.mutable.Set[Dimension]()
        val measureSet = scala.collection.mutable.Set[Measure]()
        for(ex <- fields){
          val array = ex.split(":")
          val fieldName = array(0)
          val functionName = array(1) 

          //only basic functions are supported as of now. 
          //Extend this to support custom udf of hive as well.
          if(!functionName.isEmpty()){
            measureMap.get(fieldName) match{
            case None => throw new RuntimeException("Aggregation functions are not supported on Dimension.")
            case _ => 		
            }  
          }
          dimensionMap.get(fieldName) match{
          case Some(dimension) => 
            dimensionSet.+=(dimension)
          case None =>
            measureMap.get(fieldName) match{
            case None => throw new Exception("Field not registered.")
            case Some(measure) => measureSet.+=(measure)
            }
          }
        }
        
        val _$cubeProperties = c.getProperties()
        val _$propertyMap = _$cubeProperties.split(",").map(x => {
          val xtoken = x.split(":")
          (xtoken(0).trim, xtoken(1).trim)
        })
        val propertyMap = _$propertyMap.toMap
        
        val levelpolicymap = Utility12345.getLevelPointMap(getProperty(propertyMap, defaultPropertyMap, ConfConstants.levelpolicymap, cubeName))
        val timeserieslevelpolicymap = Utility12345.getLevelPointMap(getProperty(propertyMap, defaultPropertyMap, ConfConstants.timeserieslevelpolicymap, cubeName))
        val Gnx = getProperty(propertyMap, defaultPropertyMap, ConfConstants.basegranularity, cubeName)
        val granularity = TimeGranularity.getTimeGranularityByName(Gnx).getOrElse(throw new RuntimeException("Granularity doesnot exist " + Gnx))
        val cube = Cube(cubeName, DimensionSet(dimensionSet.toSet), MeasureSet(measureSet.toSet), granularity, true, levelpolicymap, timeserieslevelpolicymap)
        cubeMap.put(cubeName, cube)
        cube
      }
    cubeList.++=(list)
  }
  
  private def getProperty(propertyMap: Map[String, String], defaultPropertyMap: Map[String, String], name: String, nmCube: String) = {
    
    propertyMap.getOrElse(name, defaultPropertyMap.getOrElse(name, throw new RuntimeException(s"The configurtion $name should be done for cube $nmCube")))
  }
  
  private [workflow] def parseSql(sql: String) = { 
    
    val sqlTableGetter = new SQLTableGetter
    val pm = SQLParserFactory.getParserManager();
    val statement = pm.parse(new StringReader(sql));
    val list = sqlTableGetter.getTableList(statement.asInstanceOf[Select]).toList.asInstanceOf[List[String]]
    val startTime = 0l // getStartTime
    val endTime = 0l // getEndTime
    (list, (startTime, endTime))
  }
  
  private def checkQLValidation(sqlContext: SQLContext, qltype: QLType) = { 
    
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
  }
}

