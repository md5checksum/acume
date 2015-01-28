package com.guavus.rubix.cache.util

import scala.collection.mutable.Map
/**
 * @author pankaj.arora
 */
object CacheUtils {
  private var threadLocal: ThreadLocal[Map[Any, Any]] = new ThreadLocal[Map[Any, Any]]()


  def setCachedElement(cachedElement: Map[Any, Any]) {
    threadLocal.set(cachedElement)
  }

  def getCachedElement(): Map[Any, Any] = threadLocal.get

  def recycle() {
    threadLocal.set(null)
  }

}