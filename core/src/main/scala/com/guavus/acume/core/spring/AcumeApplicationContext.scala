package com.guavus.acume.core.spring

import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import com.guavus.acume.core.configuration.AcumeAppConfig

class AcumeApplicationContext(ctx : ApplicationContext) {

  def getBean[T](clazz : Class[T]) : T = {
        return ctx.getBean(clazz);
    }
}


object AcumeApplicationContext {
	val logger = LoggerFactory.getLogger(classOf[AcumeApplicationContext])
	var context : AcumeApplicationContext = null
    	try{
    		 context  = new AcumeApplicationContext(new AnnotationConfigApplicationContext(classOf[AcumeAppConfig]))
    	} catch {
    	  case t : Throwable =>
    		logger.error("Error in initilizing app config: ", t);
    		throw new RuntimeException(t);
    	}
}
