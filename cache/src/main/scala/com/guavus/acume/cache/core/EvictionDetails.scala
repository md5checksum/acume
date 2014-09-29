package com.guavus.acume.cache.core

import java.util.TreeMap

class EvictionDetails(var evictionPolicyName: String, var variableRetentionMap: java.util.SortedMap[Long, Int]) {

  private var memoryEvictionThresholdCount: Int = Int.MinValue
  def this(){
    this("", new TreeMap[Long, Int]())
  }
  def this(evictionDetails: EvictionDetails){
    this(evictionDetails.evictionPolicyName, evictionDetails.variableRetentionMap)
  }
  
  def setVariableRetentionMap(variableRetentionMap: java.util.SortedMap[Long, Int]) {
    this.variableRetentionMap = variableRetentionMap;
  }
  
  def setEvictionPolicyName(evictionPolicyName: String) {
    this.evictionPolicyName = evictionPolicyName;
  }
  
  def setMemoryEvictionThresholdCount(memoryEvictionThresholdCount: Int) {
    this.memoryEvictionThresholdCount = memoryEvictionThresholdCount
  }
}