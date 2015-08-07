package com.guavus.acume.core

import java.io.File
import java.io.FileInputStream
import java.io.InputStream

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.hbase.HBaseSQLContext
import org.apache.spark.sql.hive.HiveContext

import com.guavus.acume.core.gen.AcumeUdfs
import com.guavus.acume.core.listener.AcumeBlockManagerRemovedListener
import com.guavus.acume.core.listener.AcumeSparkListener
import com.guavus.qb.ds.DatasourceType

import javax.xml.bind.JAXBContext


object AcumeContextTraitUtil {
  
  // Initialize sparkContext, hiveContext, HbaseSQLContext only  once
  val sparkContext = new SparkContext(new SparkConf())
  sparkContext.addSparkListener(new AcumeSparkListener)
  sparkContext.addSparkListener(new AcumeBlockManagerRemovedListener)
  
  lazy val hiveContext : HiveContext = new HiveContext(sparkContext)
  lazy val hBaseSQLContext : HBaseSQLContext = new HBaseSQLContext(sparkContext)

  val acumeConf = new AcumeConf(true, "acume.ini")

  def initAcumeContextTraitFactory(datsourceNames : Array[String]) : HashMap[String, AcumeContextTrait] = {
    val acumeContextMap = HashMap[String, AcumeContextTrait]()
    
    datsourceNames.map(dsName => {
      val context: AcumeContextTrait =
        DatasourceType.getDataSourceTypeFromString(dsName.toLowerCase) match {
          case DatasourceType.CACHE =>
            new AcumeContext(dsName.toLowerCase)
          case DatasourceType.HIVE =>
            new AcumeHiveContext(dsName.toLowerCase)
          case DatasourceType.HBASE =>
            new AcumeHbaseContext(dsName.toLowerCase)
          case _ => throw new RuntimeException("wrong datasource configured")
        }
      context.init
      acumeContextMap.+=(dsName.toLowerCase -> context)
    })
    
    acumeContextMap
  }

  lazy val registerUserDefinedFunctions = {
    val xml = this.acumeConf.getUdfConfigurationxml
    val jc = JAXBContext.newInstance("com.guavus.acume.core.gen")
    val unmarsh = jc.createUnmarshaller()

    var inputstream: InputStream = null
    if (new File(xml).exists())
      inputstream = new FileInputStream(xml)
    else {
      inputstream = this.getClass().getResourceAsStream("/" + xml)
      if (inputstream == null)
        throw new RuntimeException(s"$xml file does not exists")
    }

    val acumeUdf = unmarsh.unmarshal(inputstream).asInstanceOf[AcumeUdfs]
    var udf: AcumeUdfs.UserDefined = null
    for (udf <- acumeUdf.getUserDefined()) {
      val createStatement = "create temporary function " + udf.getFunctionName() + " as '" + udf.getFullUdfclassName() + "'"
      hiveContext.sql(createStatement)
    }
  }
  
}