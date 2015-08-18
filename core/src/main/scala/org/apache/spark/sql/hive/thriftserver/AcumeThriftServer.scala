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
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import scala.collection.JavaConversions._
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import com.guavus.acume.core.configuration.ConfigFactory
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import com.guavus.acume.core.AcumeContextTrait
import org.apache.hadoop.security.UserGroupInformation

/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */

private[thriftserver] object AcumeThriftServerShim {
  val version = "0.13.1"

  def setServerUserName(sparkServiceUGI: UserGroupInformation, sparkCliService:AcumeSQLCLIService) = {
    setSuperField(sparkCliService, "serviceUGI", sparkServiceUGI)
  }
}

object AcumeThriftServer extends Logging {
  var LOG = LogFactory.getLog(classOf[HiveServer2])

  def main(args: Array[String]) {
    val optionsProcessor = new ServerOptionsProcessor("HiveThriftServer2")

    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    val ss = new SessionState(new HiveConf(classOf[SessionState]))

    // Set all properties specified via command line.
    val hiveConf: HiveConf = ss.getConf
    hiveConf.getAllProperties.toSeq.sortBy(_._1).foreach { case (k, v) =>
      logDebug(s"HiveConf var: $k=$v")
    }

    SessionState.start(ss)

    logInfo("Starting SparkContext")
   // SparkSQLEnv.init()
    SessionState.start(ss)


    try {
      val server = new AcumeThriftServer(ConfigFactory.getInstance.getBean(classOf[AcumeContextTrait]).hqlContext)
      server.init(hiveConf)
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
    val sqlCLIService = new AcumeSQLCLIService(hiveContext)
    setSuperField(this, "cliService", sqlCLIService)
    addService(sqlCLIService)

    val thriftCliService = new ThriftBinaryCLIService(sqlCLIService)
    setSuperField(this, "thriftCLIService", thriftCliService)
    addService(thriftCliService)

    initCompositeService(hiveConf)
  }
}
