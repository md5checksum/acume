package com.guavus.acume.workflow

object RequestDataType extends Enumeration {

  val Aggregate = new RequestDataType()
  
  val SQL = new RequestDataType()

  val Peak = new RequestDataType()

  val Min = new RequestDataType()

  val TimeSeries = new RequestDataType()

  class RequestDataType extends Val {

    def isAggregate(): Boolean = true
  }

  implicit def convertValue(v: Value): RequestDataType = v.asInstanceOf[RequestDataType]

/*
Original Java:
package com.guavus.rubix.workflow;

|**
 * Created by IntelliJ IDEA.
 * User: vineet.mittal
 * Date: Dec 22, 2010
 * Time: 12:41:28 PM
 * To change this template use File | Settings | File Templates.
 *|
public enum RequestDataType {
    Aggregate, Peak, Min
    , TimeSeries{
		@Override
		public boolean isAggregate() {
			return false;
		}
    };
    
    public boolean isAggregate() {
    	return true;
    }
}

*/
}