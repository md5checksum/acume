package com.guavus.equinox.launch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.io.NullWritable
import com.guavus.equinox.utility._
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletRequest
import javax.servlet.ServletException
import java.io.IOException
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import com.guavus.equinox.configuration.EquinoxConfiguration
import org.apache.spark.deploy.SparkSubmit
import com.guavus.equinox.utils.CustomClasspathModificationEngine
import java.io.File
import com.guavus.equinox.common.EquinoxConstants
import java.lang.RuntimeException
import org.apache.catalina.startup.Tomcat
import org.apache.catalina.Context

class EquinoxLauncher extends HttpServlet {
  
  @throws[ServletException]
  override def init() { 
    
    EquinoxConfiguration.Runmode.getValue() match { 
      
      case _ => SparkSubmitLauncher.init
      case EquinoxConstants.SPARK_YARN => SparkSubmitLauncher.init
      case x => throw new RuntimeException("The mode " + EquinoxConfiguration.Runmode.getValue() +" is not supported yet.")
    }
  }
  
  @throws[IOException]
  @throws[ServletException]
  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    val out = response.getWriter
//    out.println("<h1>" + message + "</h1>")
    out.println("---------")
//    out.println(q)
//    val baseRdd = SparkLauncher.run(q, bool)
//    -------------------- context.sqlContext.sql(request.getParameter("sqlQuery"))
    
    println(request.getParameter("sqlQuery"))
    val sparkContext = EquinoxSparkOnYarnConfiguration.get("context") match {
      case Some(x) => x.asInstanceOf[SparkContext]
      case None => null.asInstanceOf[SparkContext]
    }
    
    val $x=sparkContext.parallelize(List(1 to 10000), 2).map({ i =>
      if(scala.math.random % 2 == 0)
        0 else 1
    }).reduce(_+_)
    
    out.println($x)
    
//    CustomClasspathModificationEngine.addFile(new File("/opt/hadoop"))
//    val argument = "--class com.guavus.equinox.launch.Del --master yarn-client" /* --jars \" + FileWrapper.commaSeparatedFile("/data/archit/server_testing_scala/solution/WEB-INF/lib") + */ + " /data/archit/server_testing_scala/solution/WEB-INF/lib/IntelligentCache-0.1-SNAPSHOT-jar-with-guava-hive-dependencies.jar"
//    SparkSubmit.main(argument.split(" "))
    
    /*
    EquinoxConfiguration.Runmode.getValue() match { 
      
      case _ => { 
        
        val equinoxLocal = request.getParameter("sqlQuery")
        EquinoxSparkOnYarnConfiguration.set("sqlquery", equinoxLocal)
        EquinoxSparkOnYarnConfiguration.set("tx", out)
        SparkSubmitLauncher.submit
      }
      case EquinoxConstants.SPARK_YARN => { 
        
        val equinoxLocal = request.getParameter("sqlQuery")
        EquinoxSparkOnYarnConfiguration.set("sqlquery", equinoxLocal)
        SparkSubmitLauncher.submit
      }
      case x => throw new RuntimeException("The mode " + EquinoxConfiguration.Runmode.getValue() +" is not supported yet.")
    }
    * 
    * 
    */
//    for(td <- baseRdd.collect)
//      out.println(td)
//    out.println("-----")
//    out.println(baseRdd.count)
//    bool = false;
  }
  
  override def destroy() { 
    
    EquinoxConfiguration.Runmode.getValue() match { 
      
      case EquinoxConstants.SPARK_YARN => SparkSubmitLauncher.destroy
      case x => throw new RuntimeException("The mode" + EquinoxConfiguration.Runmode.getValue() +" is not supported yet.")
    }
  }
}
object EquinoxLauncher { 
  
  case class SearchPRI_InteractionEgressMeasure(id: Long, ts: Long, TTS_B: Long)
  case class SearchPRI_InteractionEgressDimension(id: Long, ts: Long, EgressProspectEntityId: Long, EgressNeighborEntityId: Long, EgressAS: Long, EgressIP: Long, EgressRTR: Long, OutgoingIF: Long, FlowDirection: Long)
  case class searchIngressCustCubeDimension(id: Long, ts: Long, IngressCustomerEntityId: Long, IngressAS: Long, IngressIP: Long, IngressRTR: Long, IncomingIF: Long, IngressRuleId: Long, FlowDirection: Long)
  case class searchIngressCustCubeMeasure(id: Long, ts: Long, TTS_B: Long, On_net_B: Long, Off_net_B: Long, Local_B: Long, Regional_B: Long, Continental_B: Long, XAtlantic_B: Long, XPacific_B: Long, XOthers_B: Long)
  
  def main(args: Array[String]): Unit = { 
    
    val sparkConf = new SparkConf
  sparkConf.set("spark.app.name", "yarn-client")
//  sparkConf.setMaster("yarn-cluster") 
  val sparkContextEquinox = new SparkContext(sparkConf) //context.sparkContextEquinox
//  FileWrapper.addLocalJar(sparkContextEquinox, "/data/archit/server_testing_scala/solution/WEB-INF/lib/")
  val sqlContext = new SQLContext(sparkContextEquinox)
      val orcFile1 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressCustCubeDimension.orc")
    val orcFile2 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressCustCubeMeasure.orc")
    
    EquinoxSparkOnYarnConfiguration.set("context", sparkContextEquinox)
    import sqlContext._
//    
    val dimensionRdd = orcFile1.map(iSearchPRI_InteractionEgressDimension).registerAsTable("isearchIngressCustCubeDimension")
    val measureRdd = orcFile2.map(iSearchPRI_InteractionEgressMeasure).registerAsTable("isearchIngressCustCubeMeasure")
//    
    cacheTable("isearchIngressCustCubeDimension")
    cacheTable("isearchIngressCustCubeMeasure")

    val x_ = sqlContext.sql("select id from isearchIngressCustCubeDimension")

    val x$ = sparkContextEquinox.parallelize(List(1 to 10000), 2).map({ i =>
      if(scala.math.random % 2 == 0)
        0 else 1
    }).reduce(_+_)
    
    println(x$)
    val tomcat = new Tomcat();
    tomcat.setPort(38080);
    tomcat.addWebapp("", new File("/data/archit/server_testing_scala/solution").getAbsolutePath())
//    File docBase
    tomcat.start();
    tomcat.getServer().await();
//    val rootCtx: Context = tomcat.addContext("/app", base.getAbsolutePath());
//    for(_$x <- x_.collect)
//      println(_$x)
    
    sparkContextEquinox.stop();
  
//	  val tomcat = new Tomcat();
//	  tomcat.setPort(38080);
//	  val base: File = new File(System.getProperty("java.io.tmpdir"));
////	  val rootCtx: Context = tomcat.addContext("/app", base.getAbsolutePath());
////	  Tomcat.addServlet(rootCtx, "equinox", new EquinoxLauncher());
////	  Tomcat.addServletMapping("/date", "dateServlet");
//	  tomcat.addWebapp("/", new File("/data/archit/server_testing_scala/solution").getAbsolutePath())
//	  
//	  tomcat.start();
//	  tomcat.getServer().await();
////	  sparkContextEquinox.stop();
  }
  
  def stringToLong(str: String) = {
    
    try{
      str.toLong
    } catch {
      case ex: NumberFormatException => Int.MinValue 	
    }
  }
   
  def iSearchPRI_InteractionEgressDimension(tuple: (NullWritable, OrcStruct)) = { 
      
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressCustCubeDimension(stringToLong(token(0)), stringToLong(token(1)), stringToLong(token(2)), stringToLong(token(3)), stringToLong(token(4)), stringToLong(token(5)), stringToLong(token(6)), stringToLong(token(7)), stringToLong(token(8)))
  }
  
  def iSearchPRI_InteractionEgressMeasure(tuple: (NullWritable, OrcStruct)) = { 
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressCustCubeMeasure(stringToLong(token(0)), stringToLong(token(1)), stringToLong(token(2)), stringToLong(token(3)), stringToLong(token(4)), stringToLong(token(5)), stringToLong(token(6)), stringToLong(token(7)), stringToLong(token(8)), stringToLong(token(9)), stringToLong(token(10)))
  }
}

object context{
  val sparkConf = new SparkConf
  sparkConf.set("spark.app.name", "yarn-client")
  sparkConf.setMaster("yarn-cluster") 
  val sparkContextEquinox = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContextEquinox)
      val orcFile1 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/archit/orc/searchIngressCustCubeDimension.orc")
    val orcFile2 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/archit/orc/searchIngressCustCubeMeasure.orc")
    import sqlContext._
    
    val dimensionRdd = orcFile1.map(SparkLauncher.iSearchPRI_InteractionEgressDimension).registerAsTable("isearchIngressCustCubeDimension")
    val measureRdd = orcFile2.map(SparkLauncher.iSearchPRI_InteractionEgressMeasure).registerAsTable("isearchIngressCustCubeMeasure")
    
    cacheTable("isearchIngressCustCubeDimension")
    cacheTable("isearchIngressCustCubeMeasure")
}

object SparkLauncher {

  val sparkConf = new SparkConf
  sparkConf.set("spark.app.name", "yarn-client")
  sparkConf.setMaster("yarn-cluster") 
  val sparkContextEquinox = new SparkContext(sparkConf)
  FileWrapper.addLocalJar(sparkContextEquinox, "/data/archit/server_testing_scala/solution/WEB-INF/lib/")
  val sqlContext = new SQLContext(sparkContextEquinox)
  case class SearchPRI_InteractionEgressMeasure(id: Long, ts: Long, TTS_B: Long)
  case class SearchPRI_InteractionEgressDimension(id: Long, ts: Long, EgressProspectEntityId: Long, EgressNeighborEntityId: Long, EgressAS: Long, EgressIP: Long, EgressRTR: Long, OutgoingIF: Long, FlowDirection: Long)
  case class searchIngressCustCubeDimension(id: Long, ts: Long, IngressCustomerEntityId: Long, IngressAS: Long, IngressIP: Long, IngressRTR: Long, IncomingIF: Long, IngressRuleId: Long, FlowDirection: Long)
  case class searchIngressCustCubeMeasure(id: Long, ts: Long, TTS_B: Long, On_net_B: Long, Off_net_B: Long, Local_B: Long, Regional_B: Long, Continental_B: Long, XAtlantic_B: Long, XPacific_B: Long, XOthers_B: Long)
  
  def main(args: Array[String]) = {
    
    val orcFile1 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/archit/orc/searchIngressCustCubeDimension.orc")
    val orcFile2 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/archit/orc/searchIngressCustCubeMeasure.orc")
    import sqlContext._
    
    val dimensionRdd = orcFile1.map(iSearchPRI_InteractionEgressDimension).registerAsTable("isearchIngressCustCubeDimension")
    val measureRdd = orcFile2.map(iSearchPRI_InteractionEgressMeasure).registerAsTable("isearchIngressCustCubeMeasure")
    
    cacheTable("isearchIngressCustCubeDimension")
    cacheTable("isearchIngressCustCubeMeasure")
    
//    val future = MainThreadPool.getExservice().submit(w);
//    sqlContext.sql("SELECT IngressRTR from isearchIngressCustCubeDimension").saveAsTextFile("/data/archit/orc/isearchIngressCustCubeDimension")
//    val resultSet2 = sqlContext.sql("SELECT * from isearchIngressCustCubeDimension").saveAsTextFile("/data/archit/orc/isearchIngressCustCubeDimension")
//    sparkContextEquinox.stop
  }
  
  def run(q: String, b: Boolean) = {
    
    if(b) main(Array[String](""))
    import sqlContext._
    sqlContext.sql(q)
  }
  
  def destroy = sparkContextEquinox.stop();
  
  def stringToLong(str: String) = {
    
    try{
      str.toLong
    } catch {
      case ex: NumberFormatException => Int.MinValue 	
    }
  }
   
  def iSearchPRI_InteractionEgressDimension(tuple: (NullWritable, OrcStruct)) = { 
      
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressCustCubeDimension(stringToLong(token(0)), stringToLong(token(1)), stringToLong(token(2)), stringToLong(token(3)), stringToLong(token(4)), stringToLong(token(5)), stringToLong(token(6)), stringToLong(token(7)), stringToLong(token(8)))
  }
  
  def iSearchPRI_InteractionEgressMeasure(tuple: (NullWritable, OrcStruct)) = { 
    val struct = tuple._2
    val field = struct.toString.substring(1)
    val l = field.length
    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
    searchIngressCustCubeMeasure(stringToLong(token(0)), stringToLong(token(1)), stringToLong(token(2)), stringToLong(token(3)), stringToLong(token(4)), stringToLong(token(5)), stringToLong(token(6)), stringToLong(token(7)), stringToLong(token(8)), stringToLong(token(9)), stringToLong(token(10)))
  }
}




