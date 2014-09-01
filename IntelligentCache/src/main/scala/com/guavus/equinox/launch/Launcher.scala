package com.guavus.equinox.launch

trait Launcher {

  def submit(): Unit
  def init(): Boolean
  def destroy(): Boolean
}