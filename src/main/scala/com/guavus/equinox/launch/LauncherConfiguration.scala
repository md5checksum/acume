package com.guavus.equinox.launch

trait LauncherConfiguration {

  def get(key: String): Option[Any]
  def set(key: String, value: Any)

}