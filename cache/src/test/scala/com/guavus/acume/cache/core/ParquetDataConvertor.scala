package com.guavus.acume.cache.core

import java.io.File
import java.io.FileInputStream
import scala.Array.canBuildFrom
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.LongType
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.StructType
import com.guavus.acume.cache.common.BaseCube
import com.guavus.acume.cache.common.ConversionToSpark
import com.guavus.acume.cache.common.DataType
import com.guavus.acume.cache.common.Dimension
import com.guavus.acume.cache.common.DimensionSet
import com.guavus.acume.cache.common.FieldType
import com.guavus.acume.cache.common.Measure
import com.guavus.acume.cache.common.MeasureSet
import com.guavus.acume.cache.gen.Acume
import com.guavus.acume.cache.utility.InsensitiveStringKeyHashMap
import javax.xml.bind.JAXBContext
import com.guavus.crux.core.Fields
import com.guavus.acume.cache.common.ConversionToCrux
import com.guavus.crux.core.TextDelimitedScheme

object ParquetDataConvertor {
  
  val dimensionMap = new InsensitiveStringKeyHashMap[Dimension]
  val measureMap = new InsensitiveStringKeyHashMap[Measure]
  val baseCubeMap = new InsensitiveStringKeyHashMap[BaseCube]
  
  def main(args: Array[String]) {
	//args = srcFilepath   
    loadXML("src/test/resources/cubedefinition.xml")
    copyDirectory(args(0))
  }
  
  def getRow(row: String) = Row.fromSeq(row.split("\t").toSeq)
  
  def getNewFile(inputpath: String, outputpath:String, cubename:String, ttype: String, sqlContext: SQLContext) = {
    
      //type == d or f 	
      var sc = sqlContext.sparkContext
      val cube = baseCubeMap.get(cubename).get
      val d = cube.dimension.dimensionSet
      val m = cube.measure.measureSet
       
	  val (schema, fields, _datatype) = if(ttype == "d") { 
	    (StructType(cube.dimension.dimensionSet.map(field => { 
	      StructField(field.getName, ConversionToSpark.convertToSparkDataType(field.getDataType), true)
	    }).toList), 
	    new Fields((1.to(d.size).map(_.toString).toArray)), 
	    d.map(x => ConversionToCrux.convertToCruxFieldDataType(x.getDataType)))
	  }
	  else { 
	    (StructType(cube.measure.measureSet.map(field => { 
	      StructField(field.getName, ConversionToSpark.convertToSparkDataType(field.getDataType), true)
	    }).toList), 
	    new Fields((1.to(m.size).map(_.toString).toArray)), 
	    m.map(x => ConversionToCrux.convertToCruxFieldDataType(x.getDataType)))
	  }
      
      val text = new TextDelimitedScheme(fields, "\t", _datatype.toArray)._getRdd(inputpath, sqlContext.sparkContext).map(x => Row.fromSeq(x.getValueArray.toSeq))
    
      sqlContext.applySchema(text, schema).saveAsParquetFile(outputpath)
      
  }
  
  def copyDirectory(filePath: String) {
   
   val src = filePath
   val dest = filePath + "/../parquetInstabase"
   
   val sparkConf = new SparkConf
   sparkConf.set("spark.master","local")
   sparkConf.set("spark.app.name","local")
   val sc = new SparkContext(sparkConf)
   val sqlContext = new SQLContext(sc)
   
   if(!(new File(src)).exists())
     System.exit(0)
   else {
     copyFolder(src, dest, sqlContext)
   } 
 }
 
  def copyFolder(srcPath: String, destPath: String, sqlContext: SQLContext) {
    
    var srcFile = new File(srcPath)
    
	if(srcFile.isDirectory()){
 
    	//list all the directory contents
    	var files = srcFile.listFiles()
    	
    	for (file <- files) {
		  //construct the src and dest file structure
		  copyFolder(file.getAbsolutePath(), destPath+"/"+(file.getName()), sqlContext);
		}
 
    }else{
      
      var dest = destPath.substring(0, destPath.lastIndexOf("/"))
      
      val fileName = srcFile.getName()
      if(srcFile.getAbsolutePath().indexOf("/d/") != -1) {
    	val cubeName = fileName.substring(0, fileName.length() - "Dimension".length() - 4)
        getNewFile(srcFile.getAbsolutePath(), dest, cubeName, "d", sqlContext)
      }
      else if (srcFile.getAbsolutePath().indexOf("/f/") != -1) {
        val cubeName = fileName.substring(0, fileName.length() - "Measure".length() - 4)
        getNewFile(srcFile.getAbsolutePath(), dest, cubeName, "f", sqlContext)
      }
    }
  }
  
  def loadXML(xml: String) = { 
    
    val jc = JAXBContext.newInstance("com.guavus.acume.cache.gen")
    val unmarsh = jc.createUnmarshaller()
    val dim_id = new Dimension("timestamp", DataType.ACLong, 0)
    val dim_ts = new Dimension("id", DataType.ACLong, 0)
    val m_id = new Measure("ts", DataType.ACLong, "none", 0)
    val m_ts = new Measure("tupleid", DataType.ACLong, "none", 0)
    val acumeCube = unmarsh.unmarshal(new FileInputStream(xml)).asInstanceOf[Acume]
   
    for(lx <- acumeCube.getFields().getField().toList) { 

      val info = lx.getInfo.split(':')
      val name = info(0).trim
      val datatype = DataType.getDataType(info(1).trim)
      val fitype = FieldType.getFieldType(info(2).trim)
      val functionName = if(info.length<4) "none" else info(3).trim	
      fitype match{
        case FieldType.Dimension => 
          dimensionMap.put(name.trim, new Dimension(name, datatype, 0))
        case FieldType.Measure => 
          measureMap.put(name.trim, new Measure(name, datatype, functionName, 0 ))
      }
    }
    
	for(c <- acumeCube.getCubes().getCube().toList) yield {
	  val cubeName = c.getName().trim
	  val fields = c.getFields().split(",").map(_.trim)
	  val dimensionSet = scala.collection.mutable.Set[Dimension]()
	  dimensionSet.+=(dim_id)
	  dimensionSet.+=(dim_ts)
	  val measureSet = scala.collection.mutable.Set[Measure]()
	  measureSet.+=(m_id)
	  measureSet.+=(m_ts)
	    
	  for(ex <- fields){
	    val fieldName = ex.trim
	    
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
	 
	  val cube = BaseCube(cubeName, DimensionSet(dimensionSet.toList), MeasureSet(measureSet.toList))
	  baseCubeMap.put(cubeName, cube)
    }
  }

}
