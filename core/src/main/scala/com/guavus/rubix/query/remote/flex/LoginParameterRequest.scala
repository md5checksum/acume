package com.guavus.rubix.query.remote.flex
import java.io.Serializable
import javax.xml.bind.annotation.XmlRootElement
import scala.reflect.{BeanProperty, BooleanBeanProperty}

/**
 * @author neeru.jaroliya
 */
@XmlRootElement
class LoginParameterRequest extends Serializable {
  
  @BeanProperty
  var userName: String = _
  
  @BeanProperty
  var password: String = _
  
   def this(userName: String, password: String) {
    this()
    this.userName = userName
    this.password = password
  }

  override def toString(): String = QueryJsonUtil.loginParameterRequestToJson(this)
  
  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((userName == null)) 0 else userName.hashCode)
    result = prime * result + (if ((password == null)) 0 else password.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[LoginParameterRequest]
    if (userName == null) {
      if (other.userName != null) return false
    } else if (userName.equals(other.userName)) return false
    if (password == null) {
      if (other.password != null) return false
    } else if (password.equals(other.password)) return false
    true
  }
  
}

