package com.guavus.acume.cache.disk.schema

trait DiskStatus 
case class NotSure extends DiskStatus
case class InCache extends DiskStatus
case class NotInCacheOnDisk(x: NoDataInsideMemory) extends DiskStatus
case class NotOnDisk extends DiskStatus
case class DoesNotExist extends DiskStatus

trait NoDataInsideMemory
case class Evicted extends NoDataInsideMemory
case class NotLoaded extends NoDataInsideMemory