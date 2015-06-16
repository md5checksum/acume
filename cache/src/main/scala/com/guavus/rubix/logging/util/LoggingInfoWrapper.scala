package com.guavus.rubix.logging.util

import LoggingInfoWrapper._
import scala.reflect.{BeanProperty, BooleanBeanProperty}

object LoggingInfoWrapper {

  val NO_MODULE_ID = "NO_MODULE_ID"
}

class LoggingInfoWrapper {

  @BeanProperty
  var transactionId: String = null

/*
Original Java:
package com.guavus.rubix.logging.util;


public class LoggingInfoWrapper {
	
	public static final String NO_MODULE_ID = "NO_MODULE_ID";

	private String transactionId = null;

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

}
*/
}