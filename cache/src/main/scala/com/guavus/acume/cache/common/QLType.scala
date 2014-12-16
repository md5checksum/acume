package com.guavus.acume.cache.common

/**
 * @author archit.thakur
 *
 */
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
  
  class QLType(val QLName: String) extends Val with Equals {
    override def toString() = QLName
    
    def canEqual(other: Any) = {
      other.isInstanceOf[com.guavus.acume.cache.common.QLType.QLType]
    }
    
    override def equals(other: Any) = {
      other match {
        case that: com.guavus.acume.cache.common.QLType.QLType => that.canEqual(QLType.this) && QLName.equals(that.QLName)
        case _ => false
      }
    }
    
    override def hashCode() = {
      val prime = 41
      prime + QLName.hashCode
    }
    
  }
  
  implicit def convertValue(v: Value): QLType = v.asInstanceOf[QLType]
}



