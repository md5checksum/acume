package com.guavus.equinox.utility;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkLoad implements Callable<JobStatus>
{
//	private JobConfig jobConfig;
//	private WorkLoadConfig config;
	private static Logger logger = LoggerFactory.getLogger(WorkLoad.class.getName());
	
	public WorkLoad(/*JobConfig jobConfig*/) {
//		this.jobConfig = jobConfig;
//		this.config = this.jobConfig.getWorkLoadConfig();
	}
	
	public JobStatus call()
	{
	    /*try {
	        String logtype = jobConfig.getWorkLoadConfig().getLogType();
	        InputSource iSource = CommonUtil.getInputSource(logtype);
	        IWork workload = iSource.getWorkLoad().newInstance();
	        return workload.performWork(jobConfig);
	    } catch (IllegalAccessException ex) {
	        logger.error("Error in calling workload", ex);
		
	    } catch (InstantiationException ex) {
	        logger.error("Error in calling workload", ex);
		}*/
		try{
			
			return JobStatus.SUCCESS;
		} catch(Exception ex) {
			
		}
	    return JobStatus.FAIL;
	}
}

