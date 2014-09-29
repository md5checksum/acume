package com.guavus.acume.common

object QLType extends Enumeration {

  val sql = new QLType("sql")
  val hql = new QLType("hql")
  
  class QLType(QLName: String) extends Val
  
  implicit def convertValue(v: Value): QLType = v.asInstanceOf[QLType]
}