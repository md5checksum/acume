package com.guavus.acume.core.configuration

import java.util.Collection
import java.util.Map
import java.util.Set

trait Config {

  def getBean[T](clazz: Class[T]): T

}