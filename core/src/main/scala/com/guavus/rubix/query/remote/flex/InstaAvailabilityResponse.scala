package com.guavus.rubix.query.remote.flex
import java.io.Serializable
import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}

/**
 * @author neeru.jaroliya
 */
@XmlRootElement
class InstaAvailabilityResponse extends Serializable{
  @BeanProperty
  var key: Long = _
  
  @BeanProperty
  var value: StartEndResponse = _
  
   def this(key: Long, value: StartEndResponse) {
    this()
    this.key = key
    this.value = value
  }
  
}
