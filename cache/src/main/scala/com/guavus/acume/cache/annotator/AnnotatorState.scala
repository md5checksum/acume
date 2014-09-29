package com.guavus.acume.cache.annotator

abstract class AnnotatorState 

case object INITED extends AnnotatorState
case object UNINIT extends AnnotatorState
case object DESTROYED extends AnnotatorState
