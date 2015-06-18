package com.guavus.acume.core.exceptions
//remove if not needed
import scala.collection.JavaConversions._

object AcumeExceptionConstants extends Enumeration {

  val USER_DISABLED = new AcumeExceptionConstants("USER_DISABLED")

  val INVALID_CREDENTIALS = new AcumeExceptionConstants("INVALID_CREDENTIALS")

  val MISSING_CREDENTIALS = new AcumeExceptionConstants("MISSING_CREDENTIALS")

  val INVALID_SESSION = new AcumeExceptionConstants("INVALID_SESSION")

  val PASSWORD_RESET_FAILED = new AcumeExceptionConstants("PASSWORD_RESET_FAILED")

  val SESSION_EXPIRED = new AcumeExceptionConstants("SESSION_EXPIRED")

  val CLUSTER_SHUTDOWN_IN_PROGRESS = new AcumeExceptionConstants("CLUSTER_SHUTDOWN_IN_PROGRESS")

  val CACHE_PRELOAD_IN_PROGRESS = new AcumeExceptionConstants("CACHE_PRELOAD_IN_PROGRESS")

  val BELOW_MINIMUM_CLUSTER = new AcumeExceptionConstants("BELOW_MINIMUM_CLUSTER")

  val VIEW_CHANGE_IN_PROGRESS = new AcumeExceptionConstants("VIEW_CHANGE_IN_PROGRESS")

  val TOPOLOGY_MISMATCH_EXCEPTION = new AcumeExceptionConstants("TOPOLOGY_MISMATCH_EXCEPTION")

  val CUBE_NOT_FOUND_EXCEPTION = new AcumeExceptionConstants("CUBE_NOT_FOUND_EXCEPTION")

  val QUERY_VALIDATION_FAILED = new AcumeExceptionConstants("QUERY_VALIDATION_FAILED")

  val INVALID_GRANULARITY = new AcumeExceptionConstants("INVALID_GRANULARITY")

  val MALFORMED_REQUEST = new AcumeExceptionConstants("MALFORMED_REQUEST")

  val UNABLE_TO_SERVE = new AcumeExceptionConstants("UNABLE_TO_SERVE")

  val CLUSTER_REBALANCE_IN_PROGRESS = new AcumeExceptionConstants("CLUSTER_REBALANCE_IN_PROGRESS")

  val FLASH_REMOVAL_IN_PROGRESS = new AcumeExceptionConstants("FLASH_REMOVAL_IN_PROGRESS")

  val BIN_REPLAY_MOVE_IN_PROGRESS = new AcumeExceptionConstants("BIN_REPLAY_MOVE_IN_PROGRESS")

  val ADAPTIVE_EVICTION_IN_PROGRESS = new AcumeExceptionConstants("ADAPTIVE_EVICTION_IN_PROGRESS")
  
  val TOO_MANY_CONNECTION_EXCEPTION = new AcumeExceptionConstants("TOO_MANY_CONNECTION_EXCEPTION")
  
  case class AcumeExceptionConstants(val name: String) extends Val {

    override def toString(): String = name
  }

  implicit def convertValue(v: Value): AcumeExceptionConstants = v.asInstanceOf[AcumeExceptionConstants]
}
