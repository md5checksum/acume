package com.guavus.rubix.query.remote.flex

import java.util.List
import scala.reflect.BeanProperty
import scala.collection.JavaConversions._

class TimeZoneInfo(@BeanProperty var rules: List[List[String]], @BeanProperty var utcOffset: Long, @BeanProperty var id: String, @BeanProperty var name: String, @BeanProperty var fullName: String, @BeanProperty var dstName: String, @BeanProperty var dstFullName: String) {

  override def toString(): String = {
    var result = ""
    for (tempRule <- rules) {
      result += tempRule.toString + "\n"
    }
    result += id + "  " + name + "  " + fullName + "  " + dstName + "  " + dstFullName + "  " + utcOffset
    result
  }
}