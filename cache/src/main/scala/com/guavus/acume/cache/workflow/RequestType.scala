package com.guavus.acume.cache.workflow

/**
 * @author archit.thakur
 *
 */
object RequestType extends Enumeration {

  val Aggregate = new RequestType("Aggregate")
  val Timeseries = new RequestType("Timeseries")
  
  class RequestType(val requestId: String) extends Val

  implicit def convertValue(v: Value): RequestType = v.asInstanceOf[RequestType]

  def getRequestType(ix: String): RequestType = {
    for(actualName <- RequestType.values){
      if(ix equals actualName.requestId)
        return actualName
    }
    Aggregate
  }
}
