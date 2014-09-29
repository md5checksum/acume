package com.guavus.acume.cache.common

trait Collection extends Serializable {
	
  def getName():String
  def hashCode():Int
  def equals(any: Any): Boolean
  
}
