package com.guavus.rubix.search

object Join extends Enumeration {

  val AND = new Join()

  val OR = new Join()

  case class Join extends Val

  implicit def convertValue(v: Value): Join = v.asInstanceOf[Join]

/*
Original Java:
package com.guavus.rubix.search;

|**
 * @author akhil swain
 * 
 *|
public enum Join {
    AND, OR;
}

*/
}