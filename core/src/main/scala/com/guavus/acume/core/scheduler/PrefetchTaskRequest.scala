package com.guavus.acume.core.scheduler

import com.guavus.acume.workflow.RequestDataType._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.acume.workflow.RequestDataType
import com.guavus.rubix.query.remote.flex.QueryRequest

class PrefetchTaskRequest {

  @BeanProperty
  var queryRequest: QueryRequest = _
  
  @BeanProperty
  var endTime: Int = _
  
  @BeanProperty
  var startTime: Int = _

  @BeanProperty
  var requestDataType: RequestDataType = _

  var cashIdentifier: String = _

  override def toString(): String = {
    val builder = new StringBuilder()
    builder.append("PrefetchTaskRequest [queryRequest=")
    builder.append(queryRequest)
    builder.append(", requestDataType=")
    builder.append(requestDataType)
    builder.append("]")
    builder.toString
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.scheduler;

import com.guavus.rubix.query.QueryRequest;
import com.guavus.rubix.workflow.RequestDataType;

|**
 * @author akhil.swain
 * 
 *|
public class PrefetchTaskRequest {
	private QueryRequest queryRequest;
	private RequestDataType requestDataType;
	public String cashIdentifier;

	|**
	 * @param queryRequest
	 *            the queryRequest to set
	 *|
	public void setQueryRequest(QueryRequest queryRequest) {
		this.queryRequest = queryRequest;
	}

	|**
	 * @return the queryRequest
	 *|
	public QueryRequest getQueryRequest() {
		return queryRequest;
	}

	|**
	 * @param requestDataType
	 *            the requestDataType to set
	 *|
	public void setRequestDataType(RequestDataType requestDataType) {
		this.requestDataType = requestDataType;
	}

	|**
	 * @return the requestDataType
	 *|
	public RequestDataType getRequestDataType() {
		return requestDataType;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PrefetchTaskRequest [queryRequest=");
		builder.append(queryRequest);
		builder.append(", requestDataType=");
		builder.append(requestDataType);
		builder.append("]");
		return builder.toString();
	}



}

*/
}