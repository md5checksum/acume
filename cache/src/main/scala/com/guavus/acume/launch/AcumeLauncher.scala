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
import java.io.File
import com.guavus.acume.common.AcumeConstants
import java.lang.RuntimeException
import org.apache.catalina.startup.Tomcat
import org.apache.catalina.loader.WebappLoader

class AcumeLauncher extends HttpServlet  with Log {

  @throws[IOException]
  @throws[ServletException]
  override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
    response.setContentType("text/html")
    val out = response.getWriter
    val x = request.getParameter("sqlQuery")
    AcumeSparkOnYarnConfiguration.set("sqlquery", x)
    val list = SQLHelper.getTables(x)
    val _$SQLContext = AcumeSparkOnYarnConfiguration.get("sqlcontext")
    val sqlContext = _$SQLContext match { 
      
      case Some(x) => _$SQLContext.get.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SQL Context could not be initialized.")
    }
    val data = sqlContext.sql(x).collect
    for(_data <- data)
      out.println(_data)
//    val y = request.getParameter("newTable")
//    if(y.equals("y")){
//      TableCreator._create()
//    }
  }
}

object AcumeLauncher { 

  def main(args: Array[String]) = {
    
    val sparkConf = new SparkConf
    sparkConf.set("spark.app.name", "Acume")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    
    AcumeSparkOnYarnConfiguration.set("sqlcontext", sqlContext)
    
//    TableCreator.createTables();
     val tomcat = new Tomcat();
    tomcat.setPort(38080);
    val _$context = //tomcat.addContext("", "/data/context")
       tomcat.addWebapp("", new File("/data/archit/server_testing_scala/solution").getAbsolutePath())
    val solrLoader = new WebappLoader(classOf[WebappLoader].getClassLoader());
    _$context.setLoader(solrLoader);
    
//    tomcat.addServlet("", "servletName", "com.guavus.acume.launch.AcumeLauncher")
//    _$context.addServletMapping("/url","servletName")
    
    tomcat.start();
    tomcat.getServer().await();
  }
  
  def destroy = { 
    
    val config = AcumeSparkOnYarnConfiguration.get("sqlcontext")
    val sqlContext = config match { 
      
      case Some(sqlContext) => config.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SQLContext could not be initialized.")
    }
    sqlContext.sparkContext.stop
  }
}










