package com.guavus.acume.annotator

abstract class AnnotatorState 

case object INITED extends AnnotatorState
case object UNINIT extends AnnotatorState
case object DESTROYED extends AnnotatorState
