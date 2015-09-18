package com.guavus.rubix.query.remote.flex

import java.io.Serializable
import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}

/**
 * @author neeru.jaroliya
 */
@XmlRootElement
class StartEndResponse extends Serializable{
  @BeanProperty
  var startTime: Long =_
  
  @BeanProperty
  var endTime: Long =_
  
  def this(startTime: Long, endTime: Long) {
    this()
    this.startTime = startTime
    this.endTime = endTime
  }
  
}