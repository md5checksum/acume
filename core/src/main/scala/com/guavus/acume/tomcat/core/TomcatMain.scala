package com.guavus.acume.tomcat.core

import org.apache.catalina.startup.Catalina

/**
 * @author pankaj.arora
 * Main program from where tomcat instance comes up
 */
object TomcatMain {

  def main(args: Array[String]) {
    startTomcatAndWait()
  }

  /**
   * Uses standard way to start tomcat. Directory struture of tomcat is followed to get configuration from conf. It looks for conf folder in catalina.base directory to load server.xml and other properties
   * This must be called as the last as it does not return the control back
   */
  def startTomcatAndWait() {
    val catalina = new Catalina()
    catalina.start()
    catalina.await()
  }

  /**
   * Uses standard way to start tomcat. Directory struture of tomcat is followed to get configuration from conf. It looks for conf folder in catalina.base directory to load server.xml and other properties
   * This is async method and after starting tomcat returns the control back. Could be useful in writing unit tests 
   */
  def startTomcatAsync() {
    val catalina = new Catalina()
    catalina.start()
    new Thread(new Runnable() {
      def run() {
        catalina.await()
      }
    })
  }
}