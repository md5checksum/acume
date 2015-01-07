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
}