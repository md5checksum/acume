package com.guavus.acume.launch

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.io.NullWritable
import com.guavus.acume.utility._
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletResponse
import javax.servlet.http.HttpServletRequest
import javax.servlet.ServletException
import java.io.IOException
import org.apache.hadoop.hive.ql.io.orc.OrcStruct
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat
import org.apache.spark.deploy.SparkSubmit
import com.guavus.acume.utils.CustomClasspathModificationEngine
import java.io.File
import com.guavus.acume.common.EquinoxConstants
import java.lang.RuntimeException
import org.apache.catalina.startup.Tomcat
import org.apache.catalina.loader.WebappLoader

class EquinoxLauncher extends HttpServlet {
  
  @throws[ServletException]
  override def init() { 
    
    
  }
  
  @throws[IOException]
  @throws[ServletException]
  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    val out = response.getWriter
    
//    EquinoxConfiguration.Runmode.getValue() match { 
//      
//      case _ => { 
//        
//        val acumeLocal = request.getParameter("sqlQuery")
//        EquinoxSparkOnYarnConfiguration.set("sqlquery", acumeLocal)
//        EquinoxSparkOnYarnConfiguration.set("tx", out)
//        SparkSubmitLauncher.submit
//      }
//      case EquinoxConstants.SPARK_YARN => { 
//        
//        val acumeLocal = request.getParameter("sqlQuery")
//        EquinoxSparkOnYarnConfiguration.set("sqlquery", acumeLocal)
//        SparkSubmitLauncher.submit
//      }
//      case x => throw new RuntimeException("The mode " + EquinoxConfiguration.Runmode.getValue() +" is not supported yet.")
//    }
//    for(td <- baseRdd.collect)
//      out.println(td)
//    out.println("-----")
//    out.println(baseRdd.count)
//    bool = false;
    
    val x = request.getParameter("sqlQuery")
    EquinoxSparkOnYarnConfiguration.set("sqlquery", x)
    val _$SQLContext = EquinoxSparkOnYarnConfiguration.get("sqlcontext")
    val sqlContext = _$SQLContext match { 
      
      case Some(x) => _$SQLContext.get.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SQL Context could not be initialized.")
    }
    val resposeData = sqlContext.sql(x).collect
    for(data <- resposeData)
      out.println(data)
  }
  
  override def destroy() { 
    
    /*
     * 
    
    EquinoxConfiguration.Runmode.getValue() match { 
      
      case EquinoxConstants.SPARK_YARN => SparkSubmitLauncher.destroy
      case x => throw new RuntimeException("The mode" + EquinoxConfiguration.Runmode.getValue() +" is not supported yet.")
    }
    * 
    * 
    */
    EquinoxLauncher.destroy
  }
}

object EquinoxLauncher { 

  def main(args: Array[String]) = {
    
    val sparkConf = new SparkConf
    sparkConf.set("spark.app.name", "Equinox")
    val sparkContextEquinox = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContextEquinox)
    
    TableCreator.createTables();
    
    EquinoxSparkOnYarnConfiguration.set("sqlcontext", sqlContext)
    
     val tomcat = new Tomcat();
    tomcat.setPort(38080);
    val _$context = tomcat.addWebapp("", new File("/data/archit/server_testing_scala/solution").getAbsolutePath())
    val solrLoader = new WebappLoader(classOf[WebappLoader].getClassLoader());
    _$context.setLoader(solrLoader);
    tomcat.start();
    tomcat.getServer().await();
//    val orcFile1 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/archit/orc/searchIngressCustCubeDimension.orc")
//    val orcFile2 = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/archit/orc/searchIngressCustCubeMeasure.orc")
//    import sqlContext._
//    
//    val dimensionRdd = orcFile1.map(iSearchPRI_InteractionEgressDimension).registerAsTable("isearchIngressCustCubeDimension")
//    val measureRdd = orcFile2.map(iSearchPRI_InteractionEgressMeasure).registerAsTable("isearchIngressCustCubeMeasure")
//    
//    cacheTable("isearchIngressCustCubeDimension")
//    cacheTable("isearchIngressCustCubeMeasure")
  }
  
  def destroy = { 
    
    val config = EquinoxSparkOnYarnConfiguration.get("sqlcontext")
    val sqlContext = config match { 
      
      case Some(sqlContext) => config.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SQLContext could not be initialized.")
    }
    sqlContext.sparkContext.stop
  }
  
  def stringToLong(str: String) = {
    
    try{
      str.toLong
    } catch {
      case ex: NumberFormatException => Int.MinValue 	
    }
  }
   
//  def iSearchPRI_InteractionEgressDimension(tuple: (NullWritable, OrcStruct)) = { 
//      
//    val struct = tuple._2
//    val field = struct.toString.substring(1)
//    val l = field.length
//    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
//    searchIngressCustCubeDimension(stringToLong(token(0)), stringToLong(token(1)), stringToLong(token(2)), stringToLong(token(3)), stringToLong(token(4)), stringToLong(token(5)), stringToLong(token(6)), stringToLong(token(7)), stringToLong(token(8)))
//  }
//  
//  def iSearchPRI_InteractionEgressMeasure(tuple: (NullWritable, OrcStruct)) = { 
//    val struct = tuple._2
//    val field = struct.toString.substring(1)
//    val l = field.length
//    val token = field.substring(0, field.length - 2).split(',').map(_.trim)
//    searchIngressCustCubeMeasure(stringToLong(token(0)), stringToLong(token(1)), stringToLong(token(2)), stringToLong(token(3)), stringToLong(token(4)), stringToLong(token(5)), stringToLong(token(6)), stringToLong(token(7)), stringToLong(token(8)), stringToLong(token(9)), stringToLong(token(10)))
//  }
}










