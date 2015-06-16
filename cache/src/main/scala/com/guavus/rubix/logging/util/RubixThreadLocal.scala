package com.guavus.rubix.logging.util

object AcumeThreadLocal {

  val userThreadLocal = new ThreadLocal[LoggingInfoWrapper]()

  def set(user: LoggingInfoWrapper) {
    userThreadLocal.set(user)
  }

  def unset() {
    userThreadLocal.remove()
  }

  def get(): LoggingInfoWrapper = {
    userThreadLocal.get.asInstanceOf[LoggingInfoWrapper]
  }

/*
Original Java:
package com.guavus.rubix.logging.util;

public class RubixThreadLocal {

	public static final ThreadLocal<LoggingInfoWrapper> userThreadLocal = new ThreadLocal<LoggingInfoWrapper>();

	public static void set(LoggingInfoWrapper user) {
		userThreadLocal.set(user);
	}

	public static void unset() {
		userThreadLocal.remove();
	}

	public static LoggingInfoWrapper get() {
		return (LoggingInfoWrapper) userThreadLocal.get();
	}
}
*/
}