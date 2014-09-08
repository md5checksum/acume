package com.guavus.acume.utility;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainThreadPool {

private static ExecutorService exservice = null;
private static Logger logger = LoggerFactory.getLogger(MainThreadPool.class.getName());
	
	synchronized public static boolean prepThreadPool(int numThreads)
	{
		if( null == exservice){
			exservice = Executors.newFixedThreadPool(numThreads, new ModifiedThreadFactory("EquinoxThreadCollection"));
			return true;
		} else {
			logger.debug("ThreadPool already been initialised.");
			return false;
		}
	}
	
	public static ExecutorService getExservice()
	{
		return exservice;
	}
}
