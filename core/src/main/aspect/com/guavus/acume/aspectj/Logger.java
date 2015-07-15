package com.guavus.acume.aspectj;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.session.Session;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;

import com.guavus.rubix.logging.util.LoggingInfoWrapper;
import com.guavus.rubix.logging.util.RubixThreadLocal;
import com.guavus.rubix.user.management.utils.HttpUtils;
import com.guavus.acume.cache.utility.ZoneInfo;


import org.slf4j.LoggerFactory;

@Aspect
public class Logger {

    // private final int WIDTH = 2;
    // private StringBuilder indent = new StringBuilder();
    private int indent;
    private static final String NO_USER = "NO_USER";
    private static Map<String,String> loggerNames = new HashMap<String, String>();
    private static Map<String,org.slf4j.Logger> loggers = new ConcurrentHashMap<String,org.slf4j.Logger>();
    private static TimeZone utcTimeZone = getTimeZone("UTC");

    @Around("execution(public * com.guavus.acume.core.PSUserService.*(..)) || execution(public * com.guavus.acume.core.AcumeService.servAggregateQuery(..)) || execution(public * com.guavus.acume.core.AcumeService.servTimeseriesQuery(..)) || execution(public * com.guavus.acume.core.AcumeService.servAggregateMultiple(..)) || execution(public * com.guavus.acume.core.AcumeService.servTimeseriesMultiple(..)) || execution(public * com.guavus.acume.core.AcumeService.servSqlQueryMultiple(..))")
    public Object logApiRequests(ProceedingJoinPoint thisJoinPoint)
        throws Throwable {
        return logApiRequests(thisJoinPoint, true, true, true);
    }
    
    /*  Additional logging functions from rubix 
     * 
     * 
    @Around("execution(public * com.guavus.rubix.cache.RubixCacheMonitor.*(..))")
    public Object interfaces(ProceedingJoinPoint thisJoinPoint)
        throws Throwable {
        return log(thisJoinPoint, true, true, true,false);
    }

    @Around(" call( * com.guavus.rubix.query.DataService.*(..))  || execution(public * com.guavus.rubix.distributed.query.IntermediaryQueryExecutor.*(..))||call(public * com.guavus.rubix.transform.TransformationEngine.*(..)) || execution(public * com.guavus.rubix.processor.UniqueCountProcessor.execute(..)) || call(* com.guavus.rubix.workflow.Flow.*(..)) || call(* com.guavus.rubix.core.Controller.populateFromTimeseries(..)) || call(* com.guavus.rubix.core.Controller.populateFromDAO(..)) || call(public * com.guavus.rubix.core.Controller.getCubeData(..)) || call(public * com.guavus.rubix.core.Controller.getTable(..)) || call(public * com.guavus.rubix.core.Controller.populateDimensionSetFromDAO(..)) || call(public * com.guavus.rubix.core.Controller.getAllBinSources(..)) || call(public * com.guavus.rubix.configuration.Granularity.getMatchingGranularity(..)) ||  call(public * com.guavus.rubix.configuration.Filter.getMatchingFilters(..)) || call(public * com.guavus.rubix.configuration.GMF.getNearestGMF(..)) || call(private *  com.guavus.rubix.disk.RubixDiskStoreFacade.triggerEvictionFromDisk(..)) || call(private * com.guavus.rubix.cache.RubixDataCache.evictFromPrimaryCache(..)) || call(public *  com.guavus.rubix.cache.RubixDataCache.put(..))  || call(public *  com.guavus.rubix.cache.RubixDataCache.initPut(..)) ||   call(private * com.guavus.rubix.cache.RubixDataCache.evict(..)) || call(private * com.guavus.rubix.cache.RubixDataCache.evictFromFlashCache(..)) ")
    public Object tracecall(ProceedingJoinPoint thisJoinPoint) throws Throwable {
        return log(thisJoinPoint, true, true, false,false);
    }

    //@Around("call(public * com.guavus.rubix.cache.RubixCacheFactory.getInstance(..)) || call(* com.guavus.rubix.cache.SingleEntityRubixCache.get(..))")
    @Around("call(public * com.guavus.acume.cache.core.AcumeCacheFactory.getInstance(..))")
    public Object traceInput(ProceedingJoinPoint thisJoinPoint)
        throws Throwable {
        return log(thisJoinPoint, true, false, false,false);
    }

    //@Around(" call(* com.guavus.rubix.cache.TreeRubixCache.merge(..)) || call(* com.guavus.rubix.cache.TreeRubixCache.mergePathValues*(..)) || call(* com.guavus.rubix.cache.RubixCache.convertTo*(..)) || call(* com.guavus.rubix.cache.RubixCache.resolveTupleID(..)) || call(* com.guavus.rubix.cache.RubixCache.flush*(..)) || call(* com.guavus.rubix.cache.RubixCacheFactory.flush*(..)) || call(* com.guavus.rubix.cache.RubixCache.getModifiedFilters(..))|| call(* com.guavus.rubix.cache.RubixCache.getTupleIDsFor(..)) || call(public * com.guavus.rubix.dao.BaseAdapter.*(..))||call(public * com.guavus.rubix.cache.RubixCache.tryGet(..))||call(public * com.guavus.rubix.workflow.IMeasureProcessor.execute(..)) || execution(public * com.guavus.rubix.search.ISearchService.search*(..)) || execution(public * com.guavus.rubix.search.ISearchCube.loadDataToTable*(..))")
    @Around(" call(public * com.guavus.rubix.cache.RubixCache.tryGet(..))")
    public Object countersOnly(ProceedingJoinPoint thisJoinPoint)
        throws Throwable {
        return log(thisJoinPoint, false, false, false,false);
    }

    //@Around("execution(public * com.guavus.rubix.usermanagement.PSUserService.*(..)) || execution(public * com.guavus.rubix.filter.IFilterService.*(..)) || execution(public * com.guavus.rubix.query.remote.flex.RubixService.servAggregate(..)) || execution(public * com.guavus.rubix.query.remote.flex.RubixService.servTimeseries(..)) || execution(public * com.guavus.rubix.query.remote.flex.RubixService.servAggregateMultiple(..)) || execution(public * com.guavus.rubix.query.remote.flex.RubixService.servTimeseriesMultiple(..))")
    @Around("execution(public * com.guavus.acume.core.PSUserService.*(..)) || execution(public * com.guavus.acume.core.AcumeService.servAggregate(..)) || execution(public * com.guavus.acume.core.AcumeService.servTimeseries(..)) || execution(public * com.guavus.acume.core.AcumeService.servAggregateMultiple(..)) || execution(public * com.guavus.acume.core.AcumeService.servTimeseriesMultiple(..))")
    public Object logApiRequests(ProceedingJoinPoint thisJoinPoint)
        throws Throwable {
        return logApiRequests(thisJoinPoint, true, true, true);
    }
    
    
    
    
    @Around("call(protected * com.guavus.rubix.cache.RubixCache.getMissingRanges(..)) || call(protected * com.guavus.rubix.cache.RubixCache.get(..))")
	public Object logArgsResponseInInfo(ProceedingJoinPoint thisJoinPoint)
			throws Throwable {
		return log(thisJoinPoint,true,true,true,true);
	}
    
    @Around("call(public * com.guavus.rubix.query.data.AggregateTableData.mergeData(..))|| call(public * com.guavus.rubix.query.data.TimeseriesTableData.mergeData(..)) || call(public * com.guavus.rubix.workflow.Flow.mergeMeasures(..)) || call(protected boolean com.guavus.rubix.cache.RubixCache.put(..))")
	public Object logArgsInInfo(ProceedingJoinPoint thisJoinPoint)
			throws Throwable {
		return log(thisJoinPoint,true,false,true,true);
	}
    
	@Before("execution(public * com.guavus.rubix.query.data.AggregateTableData.mergeData(..)) || execution(public * com.guavus.rubix.query.data.TimeseriesTableData.mergeData(..))")
	public void logSelfBefore(JoinPoint thisJoinPoint) throws Throwable {
		Signature sign = thisJoinPoint.getSignature();
		org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(sign
				.getDeclaringTypeName());
		logger.info(getRequestID() + " {}->{}:self {}", new Object[] { indent,
				sign.getName(), thisJoinPoint.getThis() });
	}
	
	@After("execution(public * com.guavus.rubix.query.data.AggregateTableData.mergeData(..))|| execution(public * com.guavus.rubix.query.data.TimeseriesTableData.mergeData(..))")
	public void logSelfAfter(JoinPoint thisJoinPoint) throws Throwable {
		Signature sign = thisJoinPoint.getSignature();
		org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(sign
				.getDeclaringTypeName());
		logger.info(getRequestID() + " {}<-{}:self {}", new Object[] { indent,
				sign.getName(), thisJoinPoint.getThis() });
	}

    
    private Object log(ProceedingJoinPoint thisJoinPoint, boolean logArguments,
        boolean logResponse, boolean logException, boolean logReqResInInfo) throws Throwable {
        Signature sign = thisJoinPoint.getSignature();
        String signDeclaringTypeName = sign.getDeclaringTypeName();
        org.slf4j.Logger logger = loggers.get(signDeclaringTypeName);
        if(logger == null) {
        	synchronized(signDeclaringTypeName) {
        		loggers.put(signDeclaringTypeName, org.slf4j.LoggerFactory.getLogger(signDeclaringTypeName));
        	}
        	logger = loggers.get(signDeclaringTypeName);
        }
        long startMemory = Runtime.getRuntime().freeMemory();
        if (logArguments && logger.isDebugEnabled() && !logReqResInInfo){
            logger.debug(getRequestID() +" {}->{}:{}:{}", new Object[] { indent, sign.getName(), startMemory,
                thisJoinPoint.getArgs() });
        }else if(logArguments && logReqResInInfo){
        	logger.info(getRequestID() +" {}->{}:{}:{}", new Object[] { indent, sign.getName(), startMemory,
                thisJoinPoint.getArgs() });
        }
        else {
            logger.info(getRequestID() +" {}->{}:{}", new Object[] { indent, sign.getName(), startMemory});
        }
        // for (int i = 0; i < WIDTH; i++)
        // indent.append(" ");
        indent++;
        long startTime = System.currentTimeMillis();
        try {
            Object object = thisJoinPoint.proceed();
            if(logResponse){
	            if (!logReqResInInfo){
	                logger.debug(getRequestID() +" {}=={}:{}", new Object[] { indent,
	                    sign.getName(), object });
	            }else{
	            	logger.info(getRequestID() +" {}=={}:{}", new Object[] { indent,
	                     sign.getName(), object });
	            }
            }
            return object;
        } catch (Throwable t) {
            if (logException) {
                if (OutOfMemoryError.class.isInstance(t)) {
                    logger.info(getRequestID() +" FreeMemory before calling GC: {}", Runtime.getRuntime().freeMemory());
                    System.gc();
                    System.gc();
                    logger.info(getRequestID() +"FreeMemory after calling GC:  {}", Runtime.getRuntime().freeMemory());
                }
                logger.error(getRequestID() +t.getMessage(), t);
            }
            throw t;
        } finally {
            if (indent > 0) indent--;
            // if (indent.length() >= WIDTH) {
            // indent.delete(indent.length() - WIDTH, indent.length());
            // } else {
            // indent.delete(0, indent.length());
            // }
            long endMemory = Runtime.getRuntime()
                .freeMemory();
            logger.info(getRequestID() +
                "{}<-{}:{}:{}:{}",
                new Object[] { indent, sign.getName(),
                    System.currentTimeMillis() - startTime, endMemory,
                    startMemory - endMemory });
        }
    }
    */

    private String getRequestID() {
		LoggingInfoWrapper InfoWrapper = RubixThreadLocal.get();
		if(InfoWrapper!=null){
			return "["+InfoWrapper.getTransactionId()+"] ";
		}else {
			return "[NO-ID] ";
		}
	}
    
    public static String humanReadableTimeStamp(long timestampInSeconds) {
		DateFormat dateFormat = new SimpleDateFormat("MMM dd, yyyy HH:mm:ss z");
		dateFormat.setTimeZone(utcTimeZone);
		return dateFormat.format(new Date(timestampInSeconds * 1000));
	}
    
    public static TimeZone getTimeZone(String id){
		TimeZone tz = null;
		try{
			tz = new ZoneInfo(id);
		}
		catch (Exception e) {
			//logger.error("Error in reading timezone file for id : {}",id);
			tz = TimeZone.getTimeZone(id);
		}
		return tz;
	}
	
    private Object logApiRequests(ProceedingJoinPoint thisJoinPoint, boolean logArguments,
            boolean logResponse, boolean logException) throws Throwable {
            Signature sign = thisJoinPoint.getSignature();
            long startMemory = Runtime.getRuntime().freeMemory();
            String loggerName = loggerNames.get(sign.getDeclaringTypeName());
            if(loggerName == null) {
            	loggerNames.put(sign.getDeclaringTypeName()  , "API-" + sign.getDeclaringTypeName());
            }
            org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(loggerNames.get(sign.getDeclaringTypeName()));
            Session session = SecurityUtils.getSubject().getSession(false);
            String sessionId = (String)(session!=null?session.getId():NO_USER);
            if (logArguments){
                logger.info(getRequestID() +"[{}] - [{}-{}] --{}->{}:{}:{}", new Object[] {Logger.humanReadableTimeStamp(System.currentTimeMillis()/1000) , HttpUtils.getLoginInfo(true),sessionId,indent, sign.getName(), startMemory,
                    thisJoinPoint.getArgs() });
            }
            // for (int i = 0; i < WIDTH; i++)
            // indent.append(" ");
            indent++;
            long startTime = System.currentTimeMillis();
            try {
                Object object = thisJoinPoint.proceed();
                if (logResponse)
                    logger.info(getRequestID() +"[{}-{}] --{}=={}:{}", new Object[] {HttpUtils.getLoginInfo(true),sessionId,indent,
                        sign.getName(), "Some Result is returned by the query" });
                return object;
            } catch (Throwable t) {
                if (logException) {
                    if (OutOfMemoryError.class.isInstance(t)) {
                        logger.info(getRequestID() +"FreeMemory before calling GC: {}", Runtime.getRuntime().freeMemory());
                        System.gc();
                        System.gc();
                        logger.info(getRequestID() +"FreeMemory after calling GC:  {}", Runtime.getRuntime().freeMemory());
                    }
                    logger.error(getRequestID() +t.getMessage(), t);
                }
                throw t;
            } finally {
                if (indent > 0) indent--;
                // if (indent.length() >= WIDTH) {
                // indent.delete(indent.length() - WIDTH, indent.length());
                // } else {
                // indent.delete(0, indent.length());
                // }
                long endMemory = Runtime.getRuntime()
                    .freeMemory();
                logger.info(getRequestID() +
                    "[{}-{}] --{}<-{}:{}:{}:{}",
                    new Object[] { HttpUtils.getLoginInfo(true),sessionId,indent, sign.getName(),
                        System.currentTimeMillis() - startTime, endMemory,
                        startMemory - endMemory });
            }
        }

}
