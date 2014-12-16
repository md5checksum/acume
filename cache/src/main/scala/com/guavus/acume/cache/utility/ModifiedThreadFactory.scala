package com.guavus.acume.cache.utility

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ThreadFactory

/**
 * @author archit.thakur
 * 
 * @param thd
 */
class ModifiedThreadFactory extends ThreadFactory {

  val poolNumber: AtomicInteger = new AtomicInteger(1);
  val threadNumber: AtomicInteger = new AtomicInteger(1);
  
  val s: Option[SecurityManager] = Option(System.getSecurityManager())
        
  val group = s match { 
    
    case Some(x) => x.getThreadGroup()              
    case None => Thread.currentThread().getThreadGroup();
  }
  var namePrefix = "pool-" + poolNumber.getAndIncrement() + "-thread-";
  
  def this(thd: String) = { 
    
    this()
    namePrefix = thd + " - thread - ";
  }
  
  def newThread(r: Runnable) = {
     
    val t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0)
    if (t.isDaemon())
      t.setDaemon(false)
    if (t.getPriority() != Thread.NORM_PRIORITY)
      t.setPriority(Thread.NORM_PRIORITY)
    t
  }
}

