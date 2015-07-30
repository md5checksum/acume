package com.guavus.acume.cache.workflow

/**
 * @author archit.thakur
 *
 */

case class CubeKey(var name: String, val binsource: String) {
  name = name.toLowerCase
}

