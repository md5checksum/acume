package com.guavus.acume.core.exceptions

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang.RuntimeException
import com.google.common.base.Throwables

case class ErrorHandler

object ErrorHandler {
  
  private var logger: Logger = LoggerFactory.getLogger(classOf[ErrorHandler])
   
  def handleError(error : Error) {
    val message = error.getMessage
    val throwable = error.getCause
    
    throwable match {
      case r : RuntimeException =>
        logger.error("Fatal Acume Error." + message + " Exiting...", throwable)
        Throwables.propagate(throwable)
        System.exit(-1)
      case _ =>
        logger.error(message, throwable)
        Throwables.propagate(throwable)
    }
  }

}