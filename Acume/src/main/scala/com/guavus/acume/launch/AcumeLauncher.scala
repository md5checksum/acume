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
import com.guavus.acume.common.AcumeConstants
import java.lang.RuntimeException
import org.apache.catalina.startup.Tomcat
import org.apache.catalina.loader.WebappLoader

class AcumeLauncher extends HttpServlet {
  
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
//        AcumeSparkOnYarnConfiguration.set("sqlquery", acumeLocal)
//        AcumeSparkOnYarnConfiguration.set("tx", out)
//        SparkSubmitLauncher.submit
//      }
//      case EquinoxConstants.SPARK_YARN => { 
//        
//        val acumeLocal = request.getParameter("sqlQuery")
//        AcumeSparkOnYarnConfiguration.set("sqlquery", acumeLocal)
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
    AcumeSparkOnYarnConfiguration.set("sqlquery", x)
    val _$SQLContext = AcumeSparkOnYarnConfiguration.get("sqlcontext")
    val sqlContext = _$SQLContext match { 
      
      case Some(x) => _$SQLContext.get.asInstanceOf[SQLContext]
      case None => throw new RuntimeException("SQL Context could not be initialized.")
    }
    sqlContext.sql(x).map(println)
//    for(data <- resposeData)
//      out.println(data)
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
    AcumeLauncher.destroy
  }
}

object AcumeLauncher { 

  def main(args: Array[String]) = {
    
    val sparkConf = new SparkConf
    sparkConf.set("spark.app.name", "Equinox")
    val sparkContextEquinox = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContextEquinox)
    
    AcumeSparkOnYarnConfiguration.set("sqlcontext", sqlContext)
    
    val orc_searchIngressCustCubeDimension = sparkContextEquinox.newAPIHadoopFile[NullWritable, OrcStruct, OrcNewInputFormat]("/data/intelligentcache/orc/searchIngressCustCubeDimension.orc")
    TableCreator.createTables();
     val tomcat = new Tomcat();
    tomcat.setPort(38080);
    val _$context = tomcat.addWebapp("", new File("/data/archit/server_testing_scala/solution").getAbsolutePath())
    val solrLoader = new WebappLoader(classOf[WebappLoader].getClassLoader());
    _$context.setLoader(solrLoader);
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










