package com.guavus.rubix.query.remote.flex

/**
 * @author neeru.jaroliya
 */
import java.io.Serializable
import javax.xml.bind.annotation.XmlRootElement
import com.google.gson.Gson
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import java.util.ArrayList

@XmlRootElement
class ZoneInfoRequest extends Serializable {

  @BeanProperty
  var zoneInfoParams: ZoneInfoParams = _
  
  @BeanProperty
  var idList: java.util.List[String] = _
  
  def this(zoneInfoParams: ZoneInfoParams, idList: java.util.List[String]) {
    this()
    this.zoneInfoParams = zoneInfoParams
    this.idList = new ArrayList(idList)
  }

  override def toString(): String = QueryJsonUtil.zoneInfoRequestToJson(this)

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((idList == null)) 0 else idList.hashCode)
    result = prime * result + (if ((zoneInfoParams == null)) 0 else zoneInfoParams.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[ZoneInfoRequest]
    if (idList == null) {
      if (other.idList != null) return false
    } else if (idList.equals(other.idList)) return false
    if (zoneInfoParams == null) {
      if (other.zoneInfoParams != null) return false
    } else if (zoneInfoParams.equals(other.zoneInfoParams)) return false
    true
  }
  
}
