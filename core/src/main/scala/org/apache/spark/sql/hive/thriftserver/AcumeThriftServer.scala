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

package org.apache.spark.sql.hive.thriftserver

import scala.collection.JavaConversions._
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.hive.service.server.HiveServer2.ServerOptionsProcessor
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.core.configuration.ConfigFactory
import com.guavus.acume.core.AcumeContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import com.guavus.acume.core.AcumeContextTrait
import org.apache.hadoop.security.UserGroupInformation
import com.guavus.acume.core.AcumeContextTraitUtil
import org.apache.hive.service.server.{HiveServerServerOptionsProcessor, HiveServer2}
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobStart}
import org.apache.spark.sql.SQLConf
/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */

private[thriftserver] object AcumeThriftServerShim {
  val version = "0.13.1"
  var uiTab: Option[ThriftServerTab] = _
  def setServerUserName(sparkServiceUGI: UserGroupInformation, sparkCliService:AcumeSQLCLIService) = {
    setSuperField(sparkCliService, "serviceUGI", sparkServiceUGI)
  }
}

object AcumeThriftServer extends Logging {
  var LOG = LogFactory.getLog(classOf[HiveServer2])

  def main(args: Array[String]) {
    val optionsProcessor = new HiveServerServerOptionsProcessor("HiveThriftServer2")
    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    logInfo("Starting SparkContext")
    SparkSQLEnv.init()


    try {
      val server = new AcumeThriftServer(AcumeContextTraitUtil.hiveContext)
      server.init(SparkSQLEnv.hiveContext.hiveconf)
      server.start()
      logInfo("HiveThriftServer2 started")

    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    
  }
}

}

class AcumeThriftServer(hiveContext: HiveContext)
  extends HiveServer2
  with ReflectedCompositeService {

  override def init(hiveConf: HiveConf) {
    val sqlCLIService = new AcumeSQLCLIService(this,hiveContext)
    setSuperField(this, "cliService", sqlCLIService)
    addService(sqlCLIService)

    val thriftCliService = new ThriftBinaryCLIService(sqlCLIService)
    setSuperField(this, "thriftCLIService", thriftCliService)
    addService(thriftCliService)

    initCompositeService(hiveConf)
  }
}