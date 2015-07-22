package com.guavus.acume.core.spring

class AcumeResolver extends Resolver {
  
  override def getBean[T](clazz: Class[T]): T  = {
		AcumeApplicationContext.context.getBean(clazz)
  }
}