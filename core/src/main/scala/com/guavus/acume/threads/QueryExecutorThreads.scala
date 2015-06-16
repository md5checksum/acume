package com.guavus.acume.threads

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.cache.common.ConfConstants

object QueryExecutorThreads {

  private var threadPool: ExecutorService = Executors.newFixedThreadPool(new AcumeConf().getInt(ConfConstants.queryThreadPoolSize, 16), new NamedThreadPoolFactory("QueryExecutorThread"))

  def getPool(): ExecutorService = threadPool

/*
Original Java:
package com.guavus.rubix.threads;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.guavus.rubix.configuration.RubixProperties;

public class QueryExecutorThreads {
    private static ExecutorService threadPool = Executors.newFixedThreadPool(RubixProperties.AggregateThreadPoolSize.getIntValue(),new NamedThreadPoolFactory("QueryExecutorThread"));

    public static ExecutorService getPool() {
        return threadPool;
    }
}

*/
}