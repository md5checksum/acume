package com.guavus.acume.core.scheduler

import java.net.ConnectException

import scala.reflect.BeanProperty

import org.apache.thrift.TApplicationException
import org.apache.thrift.TException
import org.apache.thrift.transport.TTransportException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Throwables
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.AcumeConf
import com.guavus.acume.core.DataService
import com.guavus.rubix.user.management.utils.HttpUtils
import com.guavus.acume.workflow.RequestDataType

import QueryPrefetchTask._

object QueryPrefetchTask {

  val logger = LoggerFactory.getLogger(classOf[QueryPrefetchTask])


}

class QueryPrefetchTask(private var dataService: DataService, @BeanProperty var request: PrefetchTaskRequest, version : Int, taskManager : QueryRequestPrefetchTaskManager, acumeConf : AcumeConf) extends Runnable with Comparable[QueryPrefetchTask] {

  private val RETRY_INTERVAL_IN_MILLIS = acumeConf.getQueryPrefetchTaskRetryIntervalInMillis

  private val MAX_NO_RETRIES = acumeConf.getQueryPrefetchTaskNoOfRetries

  override def run() {
//    RubixDistribution.getInstance.setPrefetchLoggingCallId()
    logger.info("Consuming Task : {}", request)
    var retryIntervalInMillis = RETRY_INTERVAL_IN_MILLIS
    var error: Throwable = null
    var success = false
    var reTryCount = -1
    while (true) {
      if (version != taskManager.getVersion) {
        logger.info("Not executing older prefetching task as view has changed")
        //break
      }
      reTryCount += 1
      try {
        HttpUtils.setLoginInfo(acumeConf.getSuperUser)
        if (request.getRequestDataType == RequestDataType.TimeSeries) {
          dataService.servTimeseries(request.getQueryRequest)
        } else {
          dataService.servAggregate(request.getQueryRequest)
        }
        success = true
      } catch {
        case t: Throwable => {
          error = t
          logger.warn("scheduled task failed for " + request + " with retry count : " + reTryCount, t)
        }
      } finally {
        HttpUtils.recycle()
      }
      if (!success && retryExecution(error, reTryCount)) {
        try {
          Thread.sleep(retryIntervalInMillis)
          if (retryIntervalInMillis < 3600000) {
            retryIntervalInMillis *= 2
          }
          logger.info("Going to perform retry no :" + (reTryCount + 1) + " for request " + request)
        } catch {
          case e: InterruptedException => {
            logger.error("Exception while waiting to retry the task " + e)
            Throwables.propagate(e)
          }
        }
        //continue
      }
      //break
    }
    logger.info("Finished Task fetch: {}", request)
  }

  private def retryExecution(error: Throwable, reTryCount: Int): Boolean = {
    if (Utility.isCause(classOf[TException], error) && Utility.isCause(classOf[ConnectException], error)) {
      return true
    }
    if (reTryCount >= MAX_NO_RETRIES) {
      logger.error("scheduled task failed even after " + reTryCount + " retries, for " + request)
      Throwables.propagate(error)
    }
    if (Utility.isCause(classOf[TTransportException], error) || Utility.isCause(classOf[TApplicationException], error)) {
      return true
    }
    false
  }

  override def compareTo(o: QueryPrefetchTask): Int = {
    val equal = 0
    val before = -1
    val after = 1
    val otherEndTime = o.request.getQueryRequest.getEndTime.toInt
    val endTime = request.getQueryRequest.getEndTime.toInt
    val otherStartTime = o.request.getQueryRequest.getStartTime.toInt
    val startTime = request.getQueryRequest.getStartTime.toInt
    if (startTime == otherStartTime && endTime == otherEndTime) {
      if (o.request.getQueryRequest.getSubQuery == null && request.getQueryRequest.getSubQuery == null) {
        if (o.request.getRequestDataType == RequestDataType.TimeSeries) {
          return after
        } else {
          return before
        }
      } else if (o.request.getQueryRequest.getSubQuery != null && request.getQueryRequest.getSubQuery != null) {
        return after
      } else {
        if (request.getQueryRequest.getSubQuery != null && o.request.getQueryRequest.getSubQuery == null) {
          if (o.request.getQueryRequest.getResponseDimensions.containsAll(request.getQueryRequest.getSubQuery.getResponseDimensions)) {
            return after
          } else {
            return after
          }
        } else {
          if (request.getQueryRequest.getResponseDimensions.containsAll(o.request.getQueryRequest.getSubQuery.getResponseDimensions)) {
            return before
          } else {
            return before
          }
        }
      }
    }
    var priority = if ((startTime - otherStartTime) != 0) (otherStartTime - startTime) else (endTime - startTime) - (otherEndTime - otherStartTime)
    if (priority == 0) {
      priority = -1
    }
    priority
  }

  override def toString(): String = {
    "QueryPrefetchTask [request=" + request + "]"
  }

/*
Original Java:
|**
 * 
 *|
package com.guavus.rubix.scheduler;

import java.net.ConnectException;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rubix.exception.RubixException;

import com.google.common.base.Throwables;
import com.guavus.rubix.configuration.RubixProperties;
import com.guavus.rubix.core.distribution.RubixDistribution;
import com.guavus.rubix.core.distribution.TopologyMismatchException;
import com.guavus.rubix.core.distribution.ViewChangeInProgressException;
import com.guavus.rubix.exceptions.RubixExceptionConstants;
import com.guavus.rubix.query.DataService;
import com.guavus.rubix.user.management.utils.HttpUtils;
import com.guavus.rubix.util.Utility;
import com.guavus.rubix.workflow.RequestDataType;

|**
 * @author akhil.swain
 * 
 *|
public class QueryPrefetchTask implements Runnable,
Comparable<QueryPrefetchTask> {

	public static final Logger logger = LoggerFactory
			.getLogger(QueryPrefetchTask.class);
	private PrefetchTaskRequest request;
	private DataService dataService;
	private int version;
	private QueryRequestPrefetchTaskManager taskManager;
	private static final long RETRY_INTERVAL_IN_MILLIS = RubixProperties.QueryPrefetchTaskRetryIntervalInMillis.getLongValue();
	private static final int MAX_NO_RETRIES = RubixProperties.QueryPrefetchTaskNoOfRetries.getIntValue();

	public QueryPrefetchTask(DataService dataService,
			PrefetchTaskRequest request) {
		this.dataService = dataService;
		this.request = request;
	}

	public QueryPrefetchTask(DataService dataService,
			PrefetchTaskRequest taskRequest, int version,
			QueryRequestPrefetchTaskManager taskManager) {
		this(dataService,taskRequest);
		this.version = version;
		this.taskManager = taskManager;
	}

	@Override
	public void run() {
		RubixDistribution.getInstance().setPrefetchLoggingCallId();
		logger.info("Consuming Task : {}", request);
		long retryIntervalInMillis = RETRY_INTERVAL_IN_MILLIS;
		Throwable error = null;
		boolean success = false;
		int reTryCount = -1;
		while (true) {
			if(version != taskManager.getVersion()) {
				logger.info("Not executing older prefetching task as view has changed");
				break;
			}
			reTryCount++;
			try {
				HttpUtils.setLoginInfo(RubixProperties.SuperUser.getValue());
				if (request.getRequestDataType() == RequestDataType.TimeSeries) {
					dataService.servTimeseries(request.getQueryRequest());
				} else {
					dataService.servAggregate(request.getQueryRequest());
				}
				success = true;
			} catch (Throwable t) {
				error = t;
				logger.warn("scheduled task failed for " + request + " with retry count : " + reTryCount, t);
			}
			finally {
				HttpUtils.recycle();
			}
			if (!success && retryExecution(error, reTryCount)) {
				try {
					//Wait for some specified interval, before retrying
					Thread.sleep(retryIntervalInMillis);
					if (retryIntervalInMillis < 3600000) {
						//Keep multiplying this till the interval reaches around a day
						retryIntervalInMillis *= 2;
					}
					logger.info("Going to perform retry no :" + (reTryCount + 1) + " for request " + request);
				} catch (InterruptedException e) {
					logger.error("Exception while waiting to retry the task " + e);
					Throwables.propagate(e);
				}
				continue;
			}
			|* Do not remove the below statement, 
			it helps in breaking out of the loop if the operations performed above succeed *|
			break;
		}
		logger.info("Finished Task fetch: {}", request);
	}

	|**
	 * Decide whether we need to re-try the task
	 * @param success
	 * @param error
	 * @param reTryCount
	 * @return
	 *|
	private boolean retryExecution(Throwable error, int reTryCount) {
		//Keep this if clause above the retry count check as we want to indefinitely keep re-trying in case insta is down
		if (Utility.isCause(TException.class, error) && Utility.isCause(ConnectException.class, error)){
			//Connection to insta has failed(probably Insta is down)....wait and keep retrying till this succeeds
			return true;
		}
		if (reTryCount >= MAX_NO_RETRIES) {
			logger.error("scheduled task failed even after " + reTryCount + 
					" retries, for " + request);
			Throwables.propagate(error);
		}
		if (Utility.isCause(TTransportException.class, error) || Utility.isCause(TApplicationException.class, error)){
			//Received exception from Insta....wait and retry for some number of times till this succeeds
			return true;
		}else if(Utility.isCause(SuspectException.class, error)){
			logger.info("Got SuspectException while executing task retrying request");
			return true;
		} else if(Utility.isCause(ViewChangeInProgressException.class, error)) {
			logger.info("Got ViewChangeInProgressException while executing task. Retrying request");
			return true;
		}else if(Utility.isCause(TopologyMismatchException.class, error)) {
			logger.info("Got TopologyMismatchException while executing task. Retrying request");
			return true;
		}else {
			RubixException exception = Utility.getRubixException(error);
			if(exception != null && exception.getCode().equals(RubixExceptionConstants.CLUSTER_REBALANCE_IN_PROGRESS.name())){
				logger.info("Got CLUSTER_REBALANCE_IN_PROGRESS exception while executing task. Retrying request");
				return true;
			}
			if(exception != null && exception.getCode().equals(RubixExceptionConstants.VIEW_CHANGE_IN_PROGRESS.name())){
				logger.info("Got VIEW_CHANGE_IN_PROGRESS exception while executing task. Retrying request");
				return true;
			}
			if(exception != null && exception.getCode().equals(RubixExceptionConstants.BIN_REPLAY_MOVE_IN_PROGRESS.name())){
				logger.info("Got BIN_REPLAY_MOVE_IN_PROGRESS exception while executing task. Retrying request");
				return true;
			}
			if(exception != null && exception.getCode().equals(RubixExceptionConstants.ADAPTIVE_EVICTION_IN_PROGRESS.name())){
				logger.info("Got ADAPTIVE_EVICTION_IN_PROGRESS exception while executing task. Retrying request");
				return true;
			}
		}
		
		return false;
	}

	@Override
	public int compareTo(QueryPrefetchTask o) {
		final int equal = 0;
		final int before = -1;
		final int after = 1;
		int otherEndTime = (int) o.request.getQueryRequest().getEndTime();
		int endTime = (int) request.getQueryRequest().getEndTime();

		int otherStartTime = (int) o.request.getQueryRequest().getStartTime();
		int startTime = (int) request.getQueryRequest().getStartTime();
		
		if (startTime == otherStartTime && endTime == otherEndTime) {
			if (o.request.getQueryRequest().getSubQuery() == null
					&& request.getQueryRequest().getSubQuery() == null) {
				// both are top queries. time series gets priority ..if both are
				// time series..doesn't matter.
				if (o.request.getRequestDataType() == RequestDataType.TimeSeries) {
					return after;
				} else {
					return before;
				}
			} else if (o.request.getQueryRequest().getSubQuery() != null
					&& request.getQueryRequest().getSubQuery() != null) {
				// both profile queries..equal priority
				return after;
			} else {
				if (request.getQueryRequest().getSubQuery() != null
						&& o.request.getQueryRequest().getSubQuery() == null) {
					if (o.request
							.getQueryRequest()
							.getResponseDimensions()
							.containsAll(
									request.getQueryRequest().getSubQuery()
									.getResponseDimensions())) {
						return after;
					} else {
						return after;
					}
				} else {
					if (request
							.getQueryRequest()
							.getResponseDimensions()
							.containsAll(
									o.request.getQueryRequest().getSubQuery()
									.getResponseDimensions())) {
						return before;
					} else {
						return before;
					}
				}
			}
		}
			int priority = (startTime - otherStartTime)!=0?(otherStartTime - startTime):(endTime - startTime) - (otherEndTime - otherStartTime);
			if(priority == 0) {
				priority = -1;
			}
			return priority;
	}

	public PrefetchTaskRequest getRequest() {
		return request;
	}

	@Override
	public String toString() {
		return "QueryPrefetchTask [request=" + request + "]";
	}


}

*/
}