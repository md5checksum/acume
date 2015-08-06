package com.guavus.acume.util

import java.util.concurrent.BlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import GracefullShutdownExecutor._
import scala.collection.JavaConversions._
import scala.collection.mutable.MutableList
import scala.collection.mutable.ArrayBuffer

object GracefullShutdownExecutor {

  private val logger = LoggerFactory.getLogger(classOf[GracefullShutdownExecutor])
}

class GracefullShutdownExecutor(corePoolSize: Int, maximumPoolSize: Int, keepAliveTime: Long, unit: TimeUnit, workQueue: BlockingQueue[Runnable], threadFactory : ThreadFactory) extends ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory) {

  var futureTasks: ArrayBuffer[Future[_]] = _

  init()

//  def this(corePoolSize: Int, maximumPoolSize: Int, keepAliveTime: Long, unit: TimeUnit, workQueue: BlockingQueue[Runnable], threadFactory: ThreadFactory) {
//    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory)
//    init()
//  }
//
//  def this(corePoolSize: Int, maximumPoolSize: Int, keepAliveTime: Long, unit: TimeUnit, workQueue: BlockingQueue[Runnable], handler: RejectedExecutionHandler) {
//    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler)
//    init()
//  }
//
//  def this(corePoolSize: Int, maximumPoolSize: Int, keepAliveTime: Long, unit: TimeUnit, workQueue: BlockingQueue[Runnable], threadFactory: ThreadFactory, handler: RejectedExecutionHandler) {
//    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler)
//    init()
//  }

  private def init() {
    futureTasks = ArrayBuffer[Future[_]]()
  }

  override def submit(task: Runnable): Future[_] = {
    val future = super.submit(task)
    updateFutureTaskList(future)
    future
  }

  override def submit[T](task: Runnable, result: T): Future[T] = {
    val future = super.submit(task, result)
    updateFutureTaskList(future)
    future
  }

  override def submit[T](task: Callable[T]): Future[T] = {
    val future = super.submit(task)
    updateFutureTaskList(future)
    future
  }

  private def updateFutureTaskList[T](future: Future[T]) {
    synchronized {
      futureTasks.+=(future)
      var i = 0
      while (i < futureTasks.size) {
        try {
          if (futureTasks.get(i).isDone) {
            futureTasks.remove(i)
          } else {
            i += 1
          }
        } catch {
          case e: Exception => logger.debug("Recieved exception while purging completed scheduler tasks", e)
        }
      }
    }
  }

  def graceFullShutdown() {
    synchronized {
      shutdown()
      for (runnableTask <- scala.collection.mutable.LinkedList[Runnable]() ++ getQueue) {
        remove(runnableTask)
      }
      for (future <- futureTasks) {
        future.cancel(false)
      }
      futureTasks.clear()
      purge()
    }
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.util;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

|**
 * @author bhupesh.goel
 *
 *|
public class GracefullShutdownExecutor extends ThreadPoolExecutor {

	List<Future> futureTasks;
	
	private static final Logger logger = LoggerFactory
			.getLogger(GracefullShutdownExecutor.class);
	
	|**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 *|
	public GracefullShutdownExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
		init();
	}

	|**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param threadFactory
	 *|
	public GracefullShutdownExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory);
		init();
	}

	|**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param handler
	 *|
	public GracefullShutdownExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				handler);
		init();
	}

	|**
	 * @param corePoolSize
	 * @param maximumPoolSize
	 * @param keepAliveTime
	 * @param unit
	 * @param workQueue
	 * @param threadFactory
	 * @param handler
	 *|
	public GracefullShutdownExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
			RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
				threadFactory, handler);
		init();
	}
	
	private void init() {
		futureTasks = new LinkedList<Future>();
	}
	
	@Override
	public Future<?> submit(Runnable task) {
		Future<?> future = super.submit(task);
		updateFutureTaskList(future);
		return future;
	}
	
	@Override
	public <T> Future<T> submit(final Runnable task,
            final T result) {
		Future<T> future = super.submit(task, result);
		updateFutureTaskList(future);
		return future;
	}
	
	@Override
	public <T> Future<T> submit(final Callable<T> task) {
		Future<T> future = super.submit(task);		
		updateFutureTaskList(future);
		return future;
	}
	
	private synchronized <T> void updateFutureTaskList(final Future<T> future) {
		futureTasks.add(future);
		for(int i=0;i<futureTasks.size();) {
			try{
				if(futureTasks.get(i).isDone()) {
					futureTasks.remove(i);
				} else {
					i++;
				}
			} catch (Exception e) {
				logger.debug("Recieved exception while purging completed scheduler tasks", e);
			}
		}
	}
	
	|**
	 * After calling this method currently executing tasks will continue to execute but 
	 * pending to execute task will not execute.
	 *|
	public synchronized void graceFullShutdown() {
		shutdown();
		
		for(Runnable runnableTask : new LinkedList<Runnable>(getQueue())) {
			remove(runnableTask);
		}
		
		|* ThreadPoolExecutor remove method may fail to remove tasks that have been converted into 
		 * other forms before being placed on the internal queue. For example, a task entered using submit 
		 * might be converted into a form that maintains Future status. *|
		for(Future future : futureTasks) {
			future.cancel(false);
		}	
		futureTasks.clear();
		purge();				
	}
}

*/
}