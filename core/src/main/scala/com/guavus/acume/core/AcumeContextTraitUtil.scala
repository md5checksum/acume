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
import com.guavus.acume.cache.common.ConfConstants
import org.apache.hadoop.fs.Path
import com.guavus.acume.cache.disk.utility.InstaUtil
import com.guavus.acume.cache.disk.utility.BinAvailabilityPoller
import com.guavus.insta.Insta
import org.slf4j.Logger
import org.slf4j.LoggerFactory


case class AcumeContextTraitUtil()

object AcumeContextTraitUtil {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeContextTraitUtil])
   
  // Initialize sparkContext, hiveContext, HbaseSQLContext only  once
  @transient
  val sparkContext = new SparkContext(new SparkConf())
  sparkContext.addSparkListener(new AcumeSparkListener)
  sparkContext.addSparkListener(new AcumeBlockManagerRemovedListener)
  
  @transient
  val hiveContext : HiveContext = new HiveContext(sparkContext)
  
  @transient
  val hBaseSQLContext : HBaseSQLContext = new HBaseSQLContext(sparkContext)
  
  val acumeConf = new AcumeConf(true, "/acume.ini")

  def initAcumeContextTraitFactory(datsourceNames : Array[String]) : HashMap[String, AcumeContextTrait] = {
    val acumeContextMap = HashMap[String, AcumeContextTrait]()
    
    val insta = getInstaClient()
    
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
      acumeContextMap.+=(dsName.toLowerCase -> context)
    })
    
    initCheckpointDir
    chooseHiveDatabase
    BinAvailabilityPoller.init(insta)
    registerUserDefinedFunctions
    acumeContextMap
  }

  def getInstaClient() : Insta = {
		if(acumeConf.getUseInsta(DatasourceType.HBASE.toString().toLowerCase()))
			InstaUtil.initializeInstaClient(hiveContext, hBaseSQLContext)
		else
			InstaUtil.initializeInstaClient(hiveContext, null)
  }
  
  lazy  val chooseHiveDatabase = {
    try{
       logger.info("Choosing database on Hive" + acumeConf.get(ConfConstants.backendDbName))
       hiveContext.sql("use " + acumeConf.get(ConfConstants.backendDbName))
    } catch {
      case ex : Exception => throw new RuntimeException("Cannot use the database " + acumeConf.get(ConfConstants.backendDbName), ex)
    }
  }
  
  lazy val initCheckpointDir = {
    //initialize anything
    // This must be called after creating acumeContext

    def getDiskBaseDirectory = {
      var diskBaseDirectory = acumeConf.get(ConfConstants.cacheBaseDirectory) + File.separator + sparkContext.getConf.get("spark.app.name")
      diskBaseDirectory = diskBaseDirectory + "-" + acumeConf.get(ConfConstants.cacheDirectory)
      diskBaseDirectory
    }

    def deleteDirectory(dir: String) = {
      logger.info("deleting checkpoint directory " + dir)
      val path = new Path(dir)
      val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
      fs.delete(path, true)
    }

    val diskBaseDirectory = getDiskBaseDirectory

    val checkpointDirectory = diskBaseDirectory + File.separator + "checkpoint"
    deleteDirectory(checkpointDirectory)
    sparkContext.setCheckpointDir(checkpointDirectory)
    logger.info(s"setting checkpoint directory as $checkpointDirectory")

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