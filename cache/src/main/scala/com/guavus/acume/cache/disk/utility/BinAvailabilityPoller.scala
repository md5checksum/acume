package com.guavus.acume.cache.disk.utility

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.cache.utility.Utility
import com.guavus.acume.cache.workflow.AcumeCacheContextTraitUtil
import com.guavus.insta.Insta

/**
 * @author kashish.jain
 */

object BinAvailabilityPoller {
  
  var insta : Insta = null
  var service : ScheduledExecutorService = null
  private var binSourceToIntervalMap = Map[String, Map[Long, (Long, Long)]]()
  @transient private val lockObject = new Object
  
  def init(insta : Insta) {
     this.insta = insta
     service = Executors.newSingleThreadScheduledExecutor()
     service.scheduleAtFixedRate(runnable, 0, AcumeCacheContextTraitUtil.cacheConf.getInt(ConfConstants.instaAvailabilityPollInterval).get, TimeUnit.SECONDS);
  }

  val runnable = new Runnable() {
    def run() {
      synchronizedGetAndUpdateMap
    }
  };
  
  private def synchronizedGetAndUpdateMap() {
   lockObject.synchronized {
     binSourceToIntervalMap = getUpdatedBinSourceToIntervalMap
   }
  }
  
  private def getUpdatedBinSourceToIntervalMap() = {
    val persistTime = insta.getAllBinPersistedTimes
    println(persistTime)
    
    //Filtering out the intervals where starttime and endtime are 0
    var filteredPersistTime = Map[String, Map[Long, (Long, Long)]]()
    persistTime.map(binSrcToGranToAvailability => {
      val granToAvailability = binSrcToGranToAvailability._2
      //Filtering out the intervals where starttime and endtime are 0
      val filteredGranToAvailability : Map[Long, (Long, Long)] = granToAvailability.filter(x => x._2._1 != 0 || x._2._2 != 0)
      filteredPersistTime += (binSrcToGranToAvailability._1 -> filteredGranToAvailability)
    })

    val updatedBinSourceToIntervalMap = filteredPersistTime.map(binSourceToGranToAvailability => {
      val minGran = binSourceToGranToAvailability._2.filter(_._1 != -1).keys.min
      val granularityToAvailability = binSourceToGranToAvailability._2.map(granToAvailability => {
        if (granToAvailability._1 == -1) {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, minGran, Utility.newCalendar)))
        } else {
          (granToAvailability._1, (granToAvailability._2._1, Utility.getNextTimeFromGranularity(granToAvailability._2._2, granToAvailability._1, Utility.newCalendar)))
        }
      })
      (binSourceToGranToAvailability._1, granularityToAvailability ++ Map(-1L -> granularityToAvailability.get(minGran).get))
    })
    
    updatedBinSourceToIntervalMap
  }
  
  def getFirstBinPersistedTime(binSource: String): Long = {
    val granToIntervalMap = getBinSourceToIntervalMap(binSource)
    granToIntervalMap.get(-1).getOrElse(throw new IllegalArgumentException("No Data found in insta for default gran -1 and binSource :" + binSource))._1
  }

  def getLastBinPersistedTime(binSource: String): Long = {
    val granToIntervalMap = getBinSourceToIntervalMap(binSource)
    granToIntervalMap.get(-1).getOrElse(throw new IllegalArgumentException("No Data found in insta for default gran -1 and binSource :" + binSource))._2
  }

  def getBinSourceToIntervalMap(binSource: String): Map[Long, (Long, Long)] = {
    getAllBinSourceToIntervalMap.getOrElse(binSource, throw new IllegalArgumentException("No Data found for binSource " + binSource))
  }

  def getAllBinSourceToIntervalMap(): Map[String, Map[Long, (Long, Long)]] = {
    if (binSourceToIntervalMap.isEmpty) {
      // This means its happening for the first time
      synchronizedGetAndUpdateMap
    }
    binSourceToIntervalMap
  }
  
}