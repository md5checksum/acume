package com.guavus.acume.util

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable

/**
 * 
 * @author archit.thakur
 * future for sequential tasks.
 */
class AcumeCustomizedFuture[T](callable: Callable[T]) extends Future[T] {

    override def cancel(mayInterruptIfRunning: Boolean): Boolean = {
      false
    } 

    override def isCancelled() = false

    override def isDone() = true

    override def get(): T = {
      callable.call
    }

    override def get(timeout: Long, unit: TimeUnit): T = {
      get
    }

}