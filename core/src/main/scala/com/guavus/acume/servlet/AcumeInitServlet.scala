package com.guavus.acume.servlet

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.guavus.acume.core.exceptions.ErrorHandler
import com.guavus.acume.tomcat.core.AcumeMain

import javax.servlet.ServletConfig
import javax.servlet.http.HttpServlet

object AcumeInitServlet {

  private var logger: Logger = LoggerFactory.getLogger(classOf[AcumeInitServlet])
}

@SerialVersionUID(2452703157821877157L)
class AcumeInitServlet extends HttpServlet {

  override def init(servletConfig: ServletConfig) {
    try {
      AcumeMain.startAcumeComponents
    } catch {
      case th : Throwable => ErrorHandler.handleError(new Error("Acume initialization failed...", th))
    }
  }
}