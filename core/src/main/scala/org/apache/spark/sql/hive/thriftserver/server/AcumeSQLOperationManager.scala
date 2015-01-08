/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.server

import java.sql.Timestamp
import java.util.{Map => JMap}
import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math.{random, round}
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession
import org.apache.spark.Logging
import org.apache.spark.sql.{Row => SparkRow, SQLConf, SchemaRDD}
import org.apache.spark.sql.catalyst.plans.logical.SetCommand
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.hive.{HiveContext, HiveMetastoreTypes}
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils
import com.guavus.acume.core.AcumeService
import org.apache.spark.sql.hive.thriftserver.AcumeSparkExecuteStatementOperation

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
class AcumeSQLOperationManager(hiveContext: HiveContext) extends OperationManager with Logging {
    
  val handleToOperation = ReflectionUtils
    .getSuperField[JMap[OperationHandle, Operation]](this, "handleToOperation")

  // TODO: Currenlty this will grow infinitely, even as sessions expire
  val sessionToActivePool = Map[HiveSession, String]()

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val operation = new AcumeSparkExecuteStatementOperation(parentSession, statement, confOverlay)(
      hiveContext, sessionToActivePool)
    handleToOperation.put(operation.getHandle, operation)
    operation
  }
}
