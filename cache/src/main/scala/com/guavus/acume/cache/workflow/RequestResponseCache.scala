package com.guavus.acume.cache.workflow

import com.guavus.acume.cache.common.AcumeCacheConf
import com.guavus.acume.cache.common.ConfConstants
import com.google.common.cache.CacheBuilder
//
//class RequestResponseCache(acumeCacheContext: AcumeCacheContext, conf: AcumeCacheConf) {
//
//  CacheBuilder.newBuilder().concurrencyLevel(conf.get(ConfConstants.rrcacheconcurrenylevel).toInt)
//				.maximumSize(conf.get(ConfConstants.rrsize._1).toInt)
//				.build(
//						new CacheLoader<CachedQueryRequestKey, AggregateTableData>() {
//			          public AggregateTableData load(CachedQueryRequestKey input){
//			        	  return computeAggregate(input.getQueryRequest(), input.getSubQueryFilter());
//			            }
//			          });
//}