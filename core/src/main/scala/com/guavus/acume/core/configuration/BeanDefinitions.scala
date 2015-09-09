package com.guavus.acume.core.configuration

import scala.collection.mutable.HashMap
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.acume.core.DataService

/**
 * @author kashish.jain
 */

case class AcumeContextTraitMap(val a : HashMap[String, AcumeContextTrait])

case class QueryBuilderSerciceMap(val q : HashMap[String, Seq[IQueryBuilderService]])

case class DataServiceMap(val d : HashMap[String, DataService])

case class DataSourceNames(var d : Array[String])
