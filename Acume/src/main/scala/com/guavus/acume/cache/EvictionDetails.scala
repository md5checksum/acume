package com.guavus.acume.cache

import scala.collection.SortedMap

class EvictionDetails(val evictionPolicyName: String, val variableRetentionMap: SortedMap[Long, Int]) {

  def this(){
    this("", SortedMap[Long, Int]())
  }
  def this(evictionDetails: EvictionDetails){
    this(evictionDetails.evictionPolicyName, evictionDetails.variableRetentionMap)
  }
}