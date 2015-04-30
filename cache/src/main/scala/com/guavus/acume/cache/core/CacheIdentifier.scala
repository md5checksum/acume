package com.guavus.acume.cache.core

import java.io.Serializable
import scala.collection.mutable.LinkedHashMap
import org.apache.commons.lang.StringUtils
import scala.collection.JavaConversions._

@SerialVersionUID(6943648967336896581L)
/**
 * @author archit.thakur
 *
 */
class CacheIdentifier extends Serializable {

  private val id = new LinkedHashMap[String, Any]()

//  def this(cid: CacheIdentifier) {
//    this()
//  }
//
//  def this(cid: CacheIdentifier, cacheIdentifierSuffix: String) {
//    this()
//    var counter = 0
//    for ((key, value) <- cid.id) {
//      if (counter == 0 && StringUtils.isNotBlank(cacheIdentifierSuffix)) {
//        put(key + cacheIdentifierSuffix, value)
//      } else {
//        put(key, value)
//      }
//      counter += 1
//    }
//  }

  def put(key: String, value: Any): CacheIdentifier = {
    id.put(key, value)
    this
  }

  def get(key: String): Any = id.get(key).getOrElse(null)

//  def getKeyOfFirstEntry(): String = id.keySet.iterator().next()

  def size(): Int = id.size

  override def hashCode(): Int = {
    var result = 31 + id.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    if(!obj.isInstanceOf[AnyRef]) return false
    if (this.eq(obj.asInstanceOf[AnyRef])) return true
    if (obj == null) return false
    if (!(obj.isInstanceOf[CacheIdentifier])) return false
    val other = obj.asInstanceOf[CacheIdentifier]
    if (id == null) {
      if (other.id != null) return false
    } else if (id != other.id) return false
    true
  }

  override def toString(): String = id.map(x => x._1 + "=" + x._2).mkString("_")
}
