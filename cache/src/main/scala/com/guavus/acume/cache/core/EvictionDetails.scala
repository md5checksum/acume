package com.guavus.acume.cache.core

class EvictionDetails(var evictionPolicyName: String, var variableRetentionMap: Map[Long, Int]) {

  private var memoryEvictionThresholdCount: Int = Int.MinValue
  def this(){
    this("", Map[Long, Int]())
  }
  def this(evictionDetails: EvictionDetails){
    this(evictionDetails.evictionPolicyName, evictionDetails.variableRetentionMap)
  }
  
  def setVariableRetentionMap(variableRetentionMap: Map[Long, Int]) {
    this.variableRetentionMap = variableRetentionMap;
  }
  
  def setEvictionPolicyName(evictionPolicyName: String) {
    this.evictionPolicyName = evictionPolicyName;
  }
  
  def setMemoryEvictionThresholdCount(memoryEvictionThresholdCount: Int) {
    this.memoryEvictionThresholdCount = memoryEvictionThresholdCount
  }
}