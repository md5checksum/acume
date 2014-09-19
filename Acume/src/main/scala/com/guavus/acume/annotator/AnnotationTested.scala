package com.guavus.acume.annotator

class AnnotationTested extends Annotator {

  var state: AnnotatorState = UNINIT
  def init(str: String*) = { 
    
    true
  } 
  def annotate() = { 
    
    
  }
  
  def destroy() = { 
    
    true
  }
  
  def getState() = state
}