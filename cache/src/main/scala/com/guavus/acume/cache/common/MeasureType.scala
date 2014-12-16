package com.guavus.acume.cache.common

import scala.collection.JavaConversions._

/**
 * @author archit.thakur
 *
 */
object MeasureType extends Enumeration {

  val BASIC = new MeasureType()
  val DERIVED = new MeasureType()
  val DERIVED_ANNOTATED = new MeasureType()
  val DERIVED_DYNAMIC = new MeasureType()

  class MeasureType extends Val {

    def isDerivedRuntime(): Boolean = {
      if (this == DERIVED || this == DERIVED_DYNAMIC) {
        return true
      }
      false
    }

    def isDerived(): Boolean = {
      if (this == DERIVED || this == DERIVED_ANNOTATED || this == DERIVED_DYNAMIC) {
        return true
      }
      false
    }
  }

  implicit def convertValue(v: Value): MeasureType = v.asInstanceOf[MeasureType]
}
