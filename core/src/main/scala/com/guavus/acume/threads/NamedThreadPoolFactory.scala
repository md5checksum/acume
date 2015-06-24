package com.guavus.acume.threads

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger
import NamedThreadPoolFactory._

object NamedThreadPoolFactory {

  val poolNumber = new AtomicInteger(1)
}

class NamedThreadPoolFactory(poolName: String) extends ThreadFactory {

  var group = if ((s != null)) s.getThreadGroup else Thread.currentThread().getThreadGroup

  val threadNumber = new AtomicInteger(1)

  var namePrefix = poolName + "-thread-"

  var priority: Int = Thread.NORM_PRIORITY

  val s = System.getSecurityManager
  def this(poolName: String, Priority: Int) {
    this(poolName)
    val s = System.getSecurityManager
    group = if ((s != null)) s.getThreadGroup else Thread.currentThread().getThreadGroup
    namePrefix = poolName + "-thread-"
    priority = Priority
  }

  def newThread(r: Runnable): Thread = {
    val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement, 0)
    t.setDaemon(true)
    t.setPriority(priority)
    t
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.threads;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

|**
 * @author akhil.swain
 *
 *|
|**
 * The default thread factory
 *|
public class NamedThreadPoolFactory implements ThreadFactory {
	static final AtomicInteger poolNumber = new AtomicInteger(1);
	final ThreadGroup group;
	final AtomicInteger threadNumber = new AtomicInteger(1);
	final String namePrefix;
	int priority;
	
	public NamedThreadPoolFactory(String poolName) {
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
				.getThreadGroup();
		namePrefix = poolName + "-thread-";
		priority = Thread.NORM_PRIORITY;
	}

	public NamedThreadPoolFactory(String poolName, int Priority) {
		SecurityManager s = System.getSecurityManager();
		group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
				.getThreadGroup();
		namePrefix = poolName + "-thread-";
		priority = Priority;
	}
	
	public Thread newThread(Runnable r) {
		Thread t = new Thread(group, r, namePrefix
				+ threadNumber.getAndIncrement(), 0);
		t.setDaemon(true);
		t.setPriority(priority);
		return t;
	}
}

*/
}