package com.guavus.acume.core.webservice

/**
 * Http error code to be returned to caller for rest apis
 */
object HttpError {

  /**
   * Query invalid or anything related to preprocessing of query
   */
  val PRECONDITION_FAILED = 412

  /**
   * Not authorized to use access this resource
   */
  val UNAUTHORISED = 401

  /**
   * Unknown exception
   */
  val NOT_ACCEPTABLE = 406

}