package com.guavus.acume.servlet

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import com.guavus.acume.core.AcumeContextTrait
import java.io.PrintWriter

class TestServletVishal extends HttpServlet {

  override def doGet(req:HttpServletRequest , resp:HttpServletResponse) {
    
//    AcumeContextTrait.acumeContext.get.ac.acql("give me cache")
    val out = resp.getWriter()
    AcumeContextTrait.acumeContext.get.ac.acql("give me cache").schemaRDD.collect.map(out.println)
//    out.println(AcumeContextTrait.acumeContext.get.ac.acql("give me cache"))
  }
}