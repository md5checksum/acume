package com.guavus.equinox.launch

import scala.collection.mutable.HashMap

object EquinoxSparkOnYarnConfiguration {

  val config = new HashMap[String, Any]
  def get(key: String) = config.get(key)
  def set(key: String, value: Any) = config.put(key, value)

  
}