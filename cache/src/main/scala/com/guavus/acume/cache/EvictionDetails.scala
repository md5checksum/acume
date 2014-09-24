package com.guavus.acume.cache

import scala.collection.SortedMap

class EvictionDetails(var evictionPolicyName: String, var variableRetentionMap: SortedMap[Long, Int]) {

  private var memoryEvictionThresholdCount: Int = Int.MinValue
  def this(){
    this("", SortedMap[Long, Int]())
  }
  def this(evictionDetails: EvictionDetails){
    this(evictionDetails.evictionPolicyName, evictionDetails.variableRetentionMap)
  }
  
  def setVariableRetentionMap(variableRetentionMap: SortedMap[Long, Int]) {
    this.variableRetentionMap = variableRetentionMap;
  }
  
  def setEvictionPolicyName(evictionPolicyName: String) {
    this.evictionPolicyName = evictionPolicyName;
  }
  
  def setMemoryEvictionThresholdCount(memoryEvictionThresholdCount: Int) {
    this.memoryEvictionThresholdCount = memoryEvictionThresholdCount
  }
}