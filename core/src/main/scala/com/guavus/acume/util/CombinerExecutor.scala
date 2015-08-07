package com.guavus.acume.util

import java.util.Collections
import java.util.ArrayList
import java.util.{ List => JList }
import java.util.concurrent.TimeUnit
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ThreadFactory
import com.guavus.acume.core.scheduler.QueryPrefetchTaskCombiner

/**
 * This Executor is supposed to be used with Combiner threads in acume scheduler.
 * @author archit.thakur
 */
class CombinerExecutor(corePoolSize: Int, maximumPoolSize: Int, keepAliveTime: Long, unit: TimeUnit, workQueue: BlockingQueue[Runnable], threadFactory: ThreadFactory)
  extends GracefullShutdownExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory) {

  private val running: JList[QueryPrefetchTaskCombiner] = Collections.synchronizedList(new ArrayList[QueryPrefetchTaskCombiner]())
  def getRunningExecutorsRunnable = running
  override def beforeExecute(t: Thread, r: Runnable) {
    super.beforeExecute(t, r)
    running.add(r.asInstanceOf[QueryPrefetchTaskCombiner])
  }

  override def afterExecute(r: Runnable, t: Throwable) {
    super.afterExecute(r, t)
    running.remove(r.asInstanceOf[QueryPrefetchTaskCombiner])
  }
}
