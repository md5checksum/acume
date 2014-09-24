package com.guavus.acume.launch

trait Launcher {

  def submit(): Unit
  def init(): Boolean
  def destroy(): Boolean
}