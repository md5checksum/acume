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
  var start: Long =_
  
  @BeanProperty
  var end: Long =_
  
  def this(start: Long, end: Long) {
    this()
    this.start = start
    this.end = end
  }
  
}