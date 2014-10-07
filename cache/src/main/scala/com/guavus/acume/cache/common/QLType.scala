package com.guavus.acume.cache.common

object QLType extends Enumeration {

  val sql = new QLType("sql")
  val hql = new QLType("hql")
  
  def getQLType(ty: String): QLType = { 
    
    for(qx <- QLType.values){
      if(qx.QLName == ty)
        return qx
    }
    throw new RuntimeException(s"QLType $ty not supported.")
  }
  
  class QLType(val QLName: String) extends Val{
    override def toString() = QLName
  }
  
  implicit def convertValue(v: Value): QLType = v.asInstanceOf[QLType]
}



