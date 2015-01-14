package com.guavus.acume.core.scheduler

import java.util.List
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.qb.cube.schema.ICube

class PrefetchCubeConfiguration {

  @BeanProperty
  var topCube: ICube = _

  @BeanProperty
  var profileCubes: List[ICube] = _

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if ((profileCubes == null)) 0 else profileCubes.hashCode)
    result = prime * result + (if ((topCube == null)) 0 else topCube.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    if (obj == null) return false
    if (getClass != obj.getClass) return false
    val other = obj.asInstanceOf[PrefetchCubeConfiguration]
    if (profileCubes == null) {
      if (other.profileCubes != null) return false
    } else if (profileCubes != other.profileCubes) return false
    if (topCube == null) {
      if (other.topCube != null) return false
    } else if (topCube != other.topCube) return false
    true
  }

  override def toString(): String = {
    val builder = new StringBuilder()
    builder.append("PrefetchCubeConfiguration [topCube=")
    builder.append(topCube)
    builder.append(", profileCubes=")
    builder.append(profileCubes)
    builder.append("]")
    builder.toString
  }
}