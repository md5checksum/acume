package com.guavus.acume.core.scheduler

import com.guavus.qb.cube.schema.QueryBuilderSchema
import com.guavus.qb.services.AbstractQueryBuilderService

class DummyQueryBuilderService extends AbstractQueryBuilderService {

  def buildQuery(query: String): String = {
    "None"
  }
	
	def getDefaultValueForField(fieldName: String): AnyRef = {
	  "NO_DEFAULT_VALUE"
	}
	
	def getQueryBuilderSchema(): QueryBuilderSchema = null
}