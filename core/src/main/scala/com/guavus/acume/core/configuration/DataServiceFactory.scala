package com.guavus.acume.core.configuration

import scala.collection.mutable.HashMap

import com.guavus.acume.cache.common.ConfConstants
import com.guavus.acume.core.AcumeContextTrait
import com.guavus.acume.core.AcumeContextTraitUtil
import com.guavus.acume.core.DataService
import com.guavus.acume.core.DsInterpreterPolicy
import com.guavus.acume.workflow.RequestDataType
import com.guavus.qb.services.IQueryBuilderService
import com.guavus.rubix.query.remote.flex.QueryRequest

import javax.xml.bind.annotation.XmlRootElement

/**
 * @author kashish.jain
 */
object DataServiceFactory {
  
  private lazy val dataserviceMapBean = ConfigFactory.getInstance().getBean(classOf[DataServiceMap])
  private lazy val dsInterpreterPolicy = Class.forName(AcumeContextTraitUtil.acumeConf.get(ConfConstants.datasourceInterpreterPolicy)).getConstructors()(0).newInstance().asInstanceOf[DsInterpreterPolicy]
  
  def initDataServiceFactory(queryBuilderServiceMap: HashMap[String, Seq[IQueryBuilderService]], acumeContextMap : HashMap[String, AcumeContextTrait]) : HashMap[String, DataService] = {
   
    val tempMap = HashMap[String, DataService]()
    
    queryBuilderServiceMap.map(qbService => {
      val acumContext = acumeContextMap.get(qbService._1).get
      tempMap.+=(qbService._1 -> new DataService(qbService._2, acumContext, qbService._1))
    })
    
    tempMap
  }
  
  def getDataserviceInstance(queryObject : Any, requestDataType: RequestDataType.RequestDataType):  DataService = {
    requestDataType match {
      case RequestDataType.SQL => 
        dataserviceMapBean.d.get(dsInterpreterPolicy.interpretDsName(queryObject.asInstanceOf[String])).get
      case RequestDataType.Aggregate =>
        dataserviceMapBean.d.get(dsInterpreterPolicy.interpretDsName(queryObject.asInstanceOf[QueryRequest].toSql(""))).get
      case RequestDataType.TimeSeries =>
        dataserviceMapBean.d.get(dsInterpreterPolicy.interpretDsName(queryObject.asInstanceOf[QueryRequest].toSql("ts, "))).get
    }
  }
  
}