package com.guavus.acume.core

trait Collection extends Serializable {
	
  def getName():String
  def hashCode():Int
  def equals(any: Any): Boolean
  
}
