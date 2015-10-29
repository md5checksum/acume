package com.guavus.rubix.query.remote.flex

import java.util.concurrent.Callable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.guavus.rubix.logging.util.LoggingInfoWrapper
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.user.management.utils.HttpUtils
import QueryExecutor._
import scala.reflect.{BeanProperty, BooleanBeanProperty}
import com.guavus.rubix.logging.util.AcumeThreadLocal
import com.guavus.acume.core.AcumeService
import com.guavus.acume.workflow.RequestDataType
import scala.collection.mutable.HashMap
import com.guavus.acume.cache.workflow.RequestType

object QueryExecutor {

private var logger: Logger = LoggerFactory.getLogger(classOf[QueryExecutor[Any]])  
}

class QueryExecutor[T](private var acumeService: AcumeService, private var loginInfo: String, var request: Any, requestDataType : RequestType.RequestType, property: HashMap[String, Any] = null) extends Callable[T] {

  @BeanProperty
  var callId: String = _
  
  var properties: HashMap[String, Any] = null
  
  def setProperties(property: HashMap[String, Any]) {
    properties = property
  }
  
  def call(): T = {
    var response: T = null.asInstanceOf[T]
    val LoggingInfoWrapper = new LoggingInfoWrapper()
    LoggingInfoWrapper.setTransactionId(this.callId)
    AcumeThreadLocal.set(LoggingInfoWrapper)
    try {
      HttpUtils.setLoginInfo(loginInfo)
        requestDataType match {
          case RequestType.Aggregate => response = acumeService.servAggregateSingleQuery(request.asInstanceOf[QueryRequest], property).asInstanceOf[T]
          case RequestType.Timeseries => response = acumeService.servTimeseriesSingleQuery(request.asInstanceOf[QueryRequest], property).asInstanceOf[T]
          case RequestType.SQL => response = acumeService.servSingleQuery(request.asInstanceOf[String], property).asInstanceOf[T]
          case _ => throw new IllegalArgumentException("QueryExecutor does not support request type: " + requestDataType)
        }
    } finally {
      HttpUtils.recycle()
      AcumeThreadLocal.unset()
      this.callId = null
    }
    response
  }

/*
Original Java:
package com.guavus.rubix.query.remote.flex;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guavus.rubix.core.distribution.RubixDistribution;
import com.guavus.rubix.logging.util.LoggingInfoWrapper;
import com.guavus.rubix.logging.util.RubixThreadLocal;
import com.guavus.rubix.user.management.utils.HttpUtils;

public class QueryExecutor<T> implements Callable<T> {

	private static Logger logger = LoggerFactory.getLogger(QueryExecutor.class);

	private RubixService rubixService;
	private CachedQueryRequestKey request;
	private String loginInfo;
	private String callId;

	public String getCallId() {
		return callId;
	}

	public void setCallId(String callId) {
		this.callId=callId;
	}

	public QueryExecutor(RubixService rubixService, String loginInfo,
			CachedQueryRequestKey request) {
		this.rubixService = rubixService;
		this.request = request;
		this.loginInfo = loginInfo;
	}

	@SuppressWarnings("unchecked")
	public T call() {
		T response = null;
		LoggingInfoWrapper LoggingInfoWrapper = new LoggingInfoWrapper();
		LoggingInfoWrapper.setTransactionId(this.callId);
		RubixThreadLocal.set(LoggingInfoWrapper);
		try {
			HttpUtils.setLoginInfo(loginInfo);
			switch (request.getRequestDataType()) {
			case Aggregate:
				response = (T) rubixService.servAggregateInternal(request.getQueryRequest());
				break;
			case TimeSeries:
				response = (T) rubixService.servTimeseriesInternal(request.getQueryRequest());
				break;
			default:
				throw new IllegalArgumentException(
						"QueryExecutor does not support request type: " + request.getRequestDataType());
			}
		} finally {
			HttpUtils.recycle();
			RubixThreadLocal.unset();
			this.callId=null;
		}
		return response;
	}
}

*/
}