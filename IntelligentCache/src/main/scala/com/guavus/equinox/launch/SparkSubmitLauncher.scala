package com.guavus.equinox.launch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.guavus.equinox.configuration.EquinoxConfiguration
import com.guavus.equinox.utility.FileWrapper
import org.apache.spark.sql.SQLContext
import org.apache.spark.deploy.SparkSubmit
import java.lang.RuntimeException
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import java.io.PrintWriter
import javax.xml.bind.JAXBContext
import javax.xml.bind.Unmarshaller
import java.io.FileInputStream
import com.guavus.equinox.xml.Cube
import com.guavus.equinox.xml.Cube.StaticCubes.StaticCube



object ApplicationLauncher { 
  
  def main(args: Array[String]) { 

    val str = EquinoxSparkOnYarnConfiguration.get("sqlquery") match { 
      
      case Some(str) => str.asInstanceOf[String] 
      case None => ""
    }
    val sqlContext = EquinoxSparkOnYarnConfiguration.get("sqlcontext") match { 
      
      case Some(sqlContext) => sqlContext.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SQLContext has not been initialized yet.")
    }
    val xr = sqlContext.sql(str)
    val out = EquinoxSparkOnYarnConfiguration.get("tx") match { 
      
      case Some(sqlContext) => sqlContext.asInstanceOf[PrintWriter]
      case None => throw new RuntimeException("PrintWriter has not been initialized yet.")
    }
    for(xt <- xr.collect)
      out.println(xt)
  }
}
object TableCreator { 
  
  def createTables(): Unit = { 
 
    
//    val jc: JAXBContext = JAXBContext.newInstance("com.guavus.equinox.xml")
//    val unmarsh: Unmarshaller = jc.createUnmarshaller()
//    val cube = unmarsh.unmarshal(new FileInputStream(EquinoxConfiguration.CubeXml.getValue())).asInstanceOf[Cube]
//    val list = cube.getStaticCubes().getStaticCube()
//    
//    for(c <- list.toArray()) { 
//      
//      val cubeId = c.asInstanceOf[StaticCube].getCubeId()
//      val dimensionSet = c.asInstanceOf[StaticCube].getDimnesionSet()
//      val measureSet = c.asInstanceOf[StaticCube].getMeasureSet()
//    }
    
    val sqlContext = EquinoxSparkOnYarnConfiguration.get("sqlcontext") match { 
      
      case Some(sqlContext) => sqlContext.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SpaarkContext has not been initialized yet.")
    }
    val sparkContext = sqlContext.sparkContext
    val orc_searchIngressCustCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressCustCubeDimension.orc")
    val orc_searchIngressCustCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressCustCubeMeasure.orc")
    val orc_DummyCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/DummyCubeDimension.orc")
    val orc_DummyCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/DummyCubeMeasure.orc")
    val orc_SearchPRI_InteractionIngressDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/SearchPRI_InteractionIngressDimension.orc")
    val orc_SearchPRI_InteractionIngressMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/SearchPRI_InteractionIngressMeasure.orc")
    val orc_searchEgressCustCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressCustCubeDimension.orc")
    val orc_searchEgressCustCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressCustCubeMeasure.orc")
    val orc_searchEgressEntityCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressEntityCubeDimension.orc")
    val orc_searchEgressEntityCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressEntityCubeMeasure.orc")
    val orc_searchEgressPeerCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressPeerCubeDimension.orc")
    val orc_searchEgressPeerCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressPeerCubeMeasure.orc")
    val orc_searchEgressProsCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressProsCubeDimension.orc")
    val orc_searchEgressProsCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchEgressProsCubeMeasure.orc")
    val orc_SearchPRI_InteractionEgressDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/SearchPRI_InteractionEgressDimension.orc")
    val orc_SearchPRI_InteractionEgressMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/SearchPRI_InteractionEgressMeasure.orc")
    val orc_searchIngressEntityCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressEntityCubeDimension.orc")
    val orc_searchIngressEntityCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressEntityCubeMeasure.orc")
    val orc_searchIngressProsCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressProsCubeDimension.orc")
    val orc_searchIngressProsCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressProsCubeMeasure.orc")
    val orc_searchPrefixEgressCustCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixEgressCustCubeDimension.orc")
    val orc_searchPrefixEgressCustCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixEgressCustCubeMeasure.orc")
    val orc_searchPrefixEgressPeerCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixEgressPeerCubeDimension.orc")
    val orc_searchPrefixEgressPeerCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixEgressPeerCubeMeasure.orc")
    val orc_searchPrefixEgressProsCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixEgressProsCubeDimension.orc")
    val orc_searchPrefixEgressProsCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixEgressProsCubeMeasure.orc")
    val orc_searchPrefixIngressCustCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixIngressCustCubeDimension.orc")
    val orc_searchPrefixIngressCustCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixIngressCustCubeMeasure.orc")
    val orc_searchPrefixIngressPeerCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixIngressPeerCubeDimension.orc")
    val orc_searchPrefixIngressPeerCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixIngressPeerCubeMeasure.orc")
    val orc_searchPrefixIngressProsCubeDimension = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixIngressProsCubeDimension.orc")
    val orc_searchPrefixIngressProsCubeMeasure = sparkContext.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchPrefixIngressProsCubeMeasure.orc")
    
    
    import sqlContext._
    
    val searchIngressCustCubeDimensionTable = orc_searchIngressCustCubeDimension.map(CubeORC._$isearchIngressCustCubeDimension).registerAsTable("searchIngressCustCubeDimension")
val searchIngressCustCubeMeasureTable = orc_searchIngressCustCubeMeasure.map(CubeORC._$isearchIngressCustCubeMeasure).registerAsTable("searchIngressCustCubeMeasure")
val DummyCubeDimensionTable = orc_DummyCubeDimension.map(CubeORC._$iDummyCubeDimension).registerAsTable("DummyCubeDimension")
val DummyCubeMeasureTable = orc_DummyCubeMeasure.map(CubeORC._$iDummyCubeMeasure).registerAsTable("DummyCubeMeasure")
val SearchPRI_InteractionIngressDimensionTable = orc_SearchPRI_InteractionIngressDimension.map(CubeORC._$iSearchPRI_InteractionIngressDimension).registerAsTable("SearchPRI_InteractionIngressDimension")
val SearchPRI_InteractionIngressMeasureTable = orc_SearchPRI_InteractionIngressMeasure.map(CubeORC._$iSearchPRI_InteractionIngressMeasure).registerAsTable("SearchPRI_InteractionIngressMeasure")
val searchEgressCustCubeDimensionTable = orc_searchEgressCustCubeDimension.map(CubeORC._$isearchEgressCustCubeDimension).registerAsTable("searchEgressCustCubeDimension")
val searchEgressCustCubeMeasureTable = orc_searchEgressCustCubeMeasure.map(CubeORC._$isearchEgressCustCubeMeasure).registerAsTable("searchEgressCustCubeMeasure")
val searchEgressEntityCubeDimensionTable = orc_searchEgressEntityCubeDimension.map(CubeORC._$isearchEgressEntityCubeDimension).registerAsTable("searchEgressEntityCubeDimension")
val searchEgressEntityCubeMeasureTable = orc_searchEgressEntityCubeMeasure.map(CubeORC._$isearchEgressEntityCubeMeasure).registerAsTable("searchEgressEntityCubeMeasure")
val searchEgressPeerCubeDimensionTable = orc_searchEgressPeerCubeDimension.map(CubeORC._$isearchEgressPeerCubeDimension).registerAsTable("searchEgressPeerCubeDimension")
val searchEgressPeerCubeMeasureTable = orc_searchEgressPeerCubeMeasure.map(CubeORC._$isearchEgressPeerCubeMeasure).registerAsTable("searchEgressPeerCubeMeasure")
val searchEgressProsCubeDimensionTable = orc_searchEgressProsCubeDimension.map(CubeORC._$isearchEgressProsCubeDimension).registerAsTable("searchEgressProsCubeDimension")
val searchEgressProsCubeMeasureTable = orc_searchEgressProsCubeMeasure.map(CubeORC._$isearchEgressProsCubeMeasure).registerAsTable("searchEgressProsCubeMeasure")
val SearchPRI_InteractionEgressDimensionTable = orc_SearchPRI_InteractionEgressDimension.map(CubeORC._$iSearchPRI_InteractionEgressDimension).registerAsTable("SearchPRI_InteractionEgressDimension")
val SearchPRI_InteractionEgressMeasureTable = orc_SearchPRI_InteractionEgressMeasure.map(CubeORC._$iSearchPRI_InteractionEgressMeasure).registerAsTable("SearchPRI_InteractionEgressMeasure")
val searchIngressEntityCubeDimensionTable = orc_searchIngressEntityCubeDimension.map(CubeORC._$isearchIngressEntityCubeDimension).registerAsTable("searchIngressEntityCubeDimension")
val searchIngressEntityCubeMeasureTable = orc_searchIngressEntityCubeMeasure.map(CubeORC._$isearchIngressEntityCubeMeasure).registerAsTable("searchIngressEntityCubeMeasure")
val searchIngressProsCubeDimensionTable = orc_searchIngressProsCubeDimension.map(CubeORC._$isearchIngressProsCubeDimension).registerAsTable("searchIngressProsCubeDimension")
val searchIngressProsCubeMeasureTable = orc_searchIngressProsCubeMeasure.map(CubeORC._$isearchIngressProsCubeMeasure).registerAsTable("searchIngressProsCubeMeasure")
val searchPrefixEgressCustCubeDimensionTable = orc_searchPrefixEgressCustCubeDimension.map(CubeORC._$isearchPrefixEgressCustCubeDimension).registerAsTable("searchPrefixEgressCustCubeDimension")
val searchPrefixEgressCustCubeMeasureTable = orc_searchPrefixEgressCustCubeMeasure.map(CubeORC._$isearchPrefixEgressCustCubeMeasure).registerAsTable("searchPrefixEgressCustCubeMeasure")
val searchPrefixEgressPeerCubeDimensionTable = orc_searchPrefixEgressPeerCubeDimension.map(CubeORC._$isearchPrefixEgressPeerCubeDimension).registerAsTable("searchPrefixEgressPeerCubeDimension")
val searchPrefixEgressPeerCubeMeasureTable = orc_searchPrefixEgressPeerCubeMeasure.map(CubeORC._$isearchPrefixEgressPeerCubeMeasure).registerAsTable("searchPrefixEgressPeerCubeMeasure")
val searchPrefixEgressProsCubeDimensionTable = orc_searchPrefixEgressProsCubeDimension.map(CubeORC._$isearchPrefixEgressProsCubeDimension).registerAsTable("searchPrefixEgressProsCubeDimension")
val searchPrefixEgressProsCubeMeasureTable = orc_searchPrefixEgressProsCubeMeasure.map(CubeORC._$isearchPrefixEgressProsCubeMeasure).registerAsTable("searchPrefixEgressProsCubeMeasure")
val searchPrefixIngressCustCubeDimensionTable = orc_searchPrefixIngressCustCubeDimension.map(CubeORC._$isearchPrefixIngressCustCubeDimension).registerAsTable("searchPrefixIngressCustCubeDimension")
val searchPrefixIngressCustCubeMeasureTable = orc_searchPrefixIngressCustCubeMeasure.map(CubeORC._$isearchPrefixIngressCustCubeMeasure).registerAsTable("searchPrefixIngressCustCubeMeasure")
val searchPrefixIngressPeerCubeDimensionTable = orc_searchPrefixIngressPeerCubeDimension.map(CubeORC._$isearchPrefixIngressPeerCubeDimension).registerAsTable("searchPrefixIngressPeerCubeDimension")
val searchPrefixIngressPeerCubeMeasureTable = orc_searchPrefixIngressPeerCubeMeasure.map(CubeORC._$isearchPrefixIngressPeerCubeMeasure).registerAsTable("searchPrefixIngressPeerCubeMeasure")
val searchPrefixIngressProsCubeDimensionTable = orc_searchPrefixIngressProsCubeDimension.map(CubeORC._$isearchPrefixIngressProsCubeDimension).registerAsTable("searchPrefixIngressProsCubeDimension")
val searchPrefixIngressProsCubeMeasureTable = orc_searchPrefixIngressProsCubeMeasure.map(CubeORC._$isearchPrefixIngressProsCubeMeasure).registerAsTable("searchPrefixIngressProsCubeMeasure")

cacheTable("searchIngressCustCubeDimension")
cacheTable("searchIngressCustCubeMeasure")
cacheTable("DummyCubeDimension")
cacheTable("DummyCubeMeasure")
cacheTable("SearchPRI_InteractionIngressDimension")
cacheTable("SearchPRI_InteractionIngressMeasure")
cacheTable("searchEgressCustCubeDimension")
cacheTable("searchEgressCustCubeMeasure")
cacheTable("searchEgressEntityCubeDimension")
cacheTable("searchEgressEntityCubeMeasure")
cacheTable("searchEgressPeerCubeDimension")
cacheTable("searchEgressPeerCubeMeasure")
cacheTable("searchEgressProsCubeDimension")
cacheTable("searchEgressProsCubeMeasure")
cacheTable("SearchPRI_InteractionEgressDimension")
cacheTable("SearchPRI_InteractionEgressMeasure")
cacheTable("searchIngressEntityCubeDimension")
cacheTable("searchIngressEntityCubeMeasure")
cacheTable("searchIngressProsCubeDimension")
cacheTable("searchIngressProsCubeMeasure")
cacheTable("searchPrefixEgressCustCubeDimension")
cacheTable("searchPrefixEgressCustCubeMeasure")
cacheTable("searchPrefixEgressPeerCubeDimension")
cacheTable("searchPrefixEgressPeerCubeMeasure")
cacheTable("searchPrefixEgressProsCubeDimension")
cacheTable("searchPrefixEgressProsCubeMeasure")
cacheTable("searchPrefixIngressCustCubeDimension")
cacheTable("searchPrefixIngressCustCubeMeasure")
cacheTable("searchPrefixIngressPeerCubeDimension")
cacheTable("searchPrefixIngressPeerCubeMeasure")
cacheTable("searchPrefixIngressProsCubeDimension")
cacheTable("searchPrefixIngressProsCubeMeasure")

  }
}
object SparkSubmitLauncher extends Launcher {

  override def submit() = {
    
    EquinoxSparkOnYarnConfiguration.get("conf") match { 
      
      case Some(conf) => SparkSubmit.main(EquinoxSparkOnYarnConfiguration.get("conf").get.asInstanceOf[String].split(" "))
      case None => throw new RuntimeException("No options found for SparkSubmit to launch");
    }
  }
  
  override def init() = { 
    
    val sparkConf = new SparkConf
    val sparkMaster = "local"
    sparkConf.setSparkHome(EquinoxConfiguration.SPARK_HOME.getValue())
    sparkConf.set("spark.app.name", "Equinox")
    sparkConf.set("spark.master", sparkMaster)
    sparkConf.setJars(FileWrapper.commaSeparatedFile(EquinoxConfiguration.DOC_BASE.getValue() + "/WEB-INF/lib/").split(","))
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val conf = "--class com.guavus.equinox.launch.ApplicationLauncher --master local " + EquinoxConfiguration.Main_Jar.getValue
    EquinoxSparkOnYarnConfiguration.set("sqlcontext", sqlContext)
    EquinoxSparkOnYarnConfiguration.set("conf", conf)
    TableCreator.createTables()
    true
  }
  
  override def destroy() = { 
    
    val sqlContext = EquinoxSparkOnYarnConfiguration.get("sqlcontext") match { 
      
      case Some(sqlContext) => sqlContext.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SpaarkContext has not been initialized yet.")
    }
    sqlContext.sparkContext.stop();
    true
  }
}