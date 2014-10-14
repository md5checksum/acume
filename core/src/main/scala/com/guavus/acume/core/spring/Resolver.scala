package com.guavus.acume.core.spring

import java.util.Collection
import java.util.Map

trait Resolver {

  def getBean[T](clazz: Class[T]): T

}