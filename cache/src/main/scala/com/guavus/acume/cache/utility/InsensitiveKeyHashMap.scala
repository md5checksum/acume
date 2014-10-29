package com.guavus.acume.cache.utility

import scala.collection.mutable.HashMap
import scala.collection.generic.MutableMapFactory

class InsensitiveStringKeyHashMap[B] extends HashMap[String, B] {

  override def contains(key: String): Boolean = super.contains(key.toUpperCase)
  
  override def put(key: String, value: B): Option[B] = super.put(key.toUpperCase, value)
  
  override def get(key: String): Option[B] = super.get(key.toUpperCase)
}

