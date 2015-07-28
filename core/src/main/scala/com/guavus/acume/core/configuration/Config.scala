package com.guavus.acume.core.configuration

trait Config {

  def getBean[T](clazz: Class[T]): T

}